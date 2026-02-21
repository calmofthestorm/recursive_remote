# Introduction

Use a git branch as an (encrypted) upstream for git repos.

# Features

- Stores multiple repositories in a single branch of the upstream repository.
  - Each has its own objects and refs.
  - (Optionally) has a separate encryption key.
- Uses a SHA256 Merkle tree to incrementally update encrypted data.
- Each upstream branch may be clear, or symmetrically encrypted using [NaCl](http://nacl.cr.yp.to/).
- Does not rewrite history (on the upstream repository), making it compatible with branch protection.
- Shallow basis allows synchronizing large repositories without storing all common objects upstream.

# Limitations

- Only supports Linux.
- Uses ~triple the storage space (Two repositories tracking the upstream in `.git/recursive_remote`).
- Relies on sys crates: Relies on OpenSSL sys crate via [git2](https://docs.rs/git2/latest/git2/). This can make the build more brittle especially on certain platforms.
- Push force requirements are implemented in-process as an approximation of `git push` semantics, so there is some risk of divergence from git behavior in edge cases.
- No automatic garbage collection. Objects stored upstream are never removed.
- Fetching from the remote fetches all objects added since the last fetch, not just those needed.

# Comparison to [gcrypt](https://www.agwa.name/projects/git-crypt)
| Feature                      | Recursive Remote                                    | Gcrypt                      |
|------------------------------|-----------------------------------------------------|-----------------------------|
| Automatic garbage collection | No                                                  | Yes                         |
| Rewrites history             | No                                                  | Yes                         |
| Prevents implicit force push | Yes                                                 | No                          |
| Branches                     | Multiple repos, each with multiple refs, per branch | 1:1 upstream branches       |
| Encrypts branch names        | Yes                                                 | No                          |
| Space Overhead               | Triple                                              | None                        |
| Encryption                   | Per namespace / per branch                          | Per repo                    |
| Encryption Library           | [NaCl](http://nacl.cr.yp.to/)                       | [GnuPG](https://gnupg.org/) |
| Language                     | Rust                                                | shell script                |
| Lines of code                | ~2528                                               | ~970                        |
| Upstream shallow basis       | Yes                                                 | No                          |
| Cross platform               | Linux only                                          | Yes                         |

Gcrypt stores the repository in a single commit, which requires rewriting
history on push. This is slow and inefficient, and precludes using branch
protection and similar mechanisms. It also means that a race is possible between
two repositories using the same upstream. Recursive Remote never rewrites
history. This means there is no automatic garbage collection, which could be a
problem in a high churn repository, as the remote will grow without bound, but
is much faster and provides mutual exclusion to push.

Gcrypt is a simple shell script that you can just drop in your path and run.
Recursive Remote is a Rust binary that must be compiled, and depends indirectly
on OpenSSL, which can make it frustrating to build on some platforms.

Recursive Remote relies on two local repositories in `.git/recursive_remote`:
one for tracking upstream state and one for object reachability/pack history.
These can be deleted at any time, but will be recreated on next use.

Gcrypt relies on GnuPG, which in my experience is brittle to script, and tends
to rely on per-user keyrings stored centrally[^1]. Recursive Remote uses NaCL
with a simpler model where each repository's key is either stored in .git/config
or in a file (potentially in the repository itself), independent of `~/.gnupg`.
They both have their advantages, but the latter is a much better fit for my use
case. I'm open to adding GnuPG support but probably won't write it myself.

Gcrypt uses a simple 1:1 mapping between local repository refs and the encrypted
remote. Recursive Remote can store multiple refs per "namespace", and multiple
namespaces per upstream branch. In practice, this means that you can store all
your repositories on a single branch of the upstream repository.

Gcrypt is mature software that has seen considerable use over its ~10 year
lifetime. I personally have used it for dozens of repos for years without any
unexpected bugs. Recursive Remote is new software that currently has exactly one
user. Consider carefully the implications of relying on relatively unproven
software for anything that matters, especially for "stateful" software where
data loss is a risk. I'm happy with it for my use case, but it's never my only
copy of the data.

Note that I do not have a background in security, and incremental updates
necessitate a more complex data structure on the backend. Any vulnerabilities
are much more likely to come down to these structures that I designed rather
than the relative security of NaCL vs GnuPG, meaning that Gcrypt's simpler state
storage is an advantage. This is compatible with my threat model, especially
since I rely on the encryption as an additional layer of protection on top
private repositories.

My main motivation for writing this was:

- The force push semantics, which has caused me/collaborators to clobber each others changes unexpectedly several times.
- Poor performance even on modest repositories.
- Annoyance of dealing with GPG with many repositories + per-repo keys.
- Storing multiple entire repos within a single branch.

# Push semantics

Ideally, the semantics of push would be identical to git for when force is
required. Git uses a combination of whether it's fast forward and the ref/object
type (see `man git-push` for details). Recursive Remote now checks this
in-process using the object graph in `all_objects_ever_repo`, without creating a
secondary repository or shelling out (as it did previously).

Current behavior without force:

- Existing `refs/tags/*` may not be updated.
- Existing refs are only updated if both old and new targets are commits and
  the update is fast-forward.
- Symbolic ref updates are rejected.

This is intentionally conservative and should match common git workflows, but
it is an approximation rather than an exact copy of git's rules. In particular,
it is at risk of becoming stale, especially because it will not take git config
into account if that changes how this works in the future.

# Ratcheting

When first connecting to a remote, we download the current history of
the upstream branch. From then on, only fast forward updates are accepted. This
ensures that if two clients attempt to update the inner repositories stored on
that branch at the same time, one succeeds and one fails. When a push fails, the
client will fetch the current state from upstream, apply its changes, then push
again. This can always be resolved automatically unless there is a conflict
between inner branches. This is exactly the same as when it happens with vanilla
Git, and will be presented to the user for resolution in the same way.

In short, it should just work like a normal Git remote.

When cloning a branch for the first time, recursive remote has to use the SHA1,
because it does not know the SHA256 of the branch yet[^4]. From then on, it
validates all updates it receives using a SHA256 Merkle tree, meaning it will
detect any SHA1 collisions in the upstream as corruption. This is a
trust-on-first-use behavior where the initial pull relies on the security of
SHA1 in the same way as vanilla Git does, but all future updates should be
protected by SHA256.

It is vanishingly unlikely that a SHA1 collision would occur by random chance,
especially given the performance and storage overhead of recursive remote mean
that repositories are unlikely to reach the size that, say, Bup repos can. In
any case, the risk of a collision in the underlying repo is comparable to the
risk of one in the inner repo it stores (because they have a simliar number of
objects). Thus, I consider this a potential security vulnerability but not a
problem for correctness.

In practical terms, this means that if the upstream is regenerated (such as to
manually garbage collect), repos will refuse to update, failing with a
ratcheting error. You can `rm -fr .git/recursive_remote` to erase that state and
once again trust-on-first-use.

# Shallow Basis

This is somewhat analogous to git's [shallow
clone](https://github.blog/2020-12-21-get-up-to-speed-with-partial-clone-and-shallow-clone/),
except that it is the upstream, rather than the local repository, that is
shallow. A local repository may be configured to consider zero or more refs/tags
as a "shallow basis". This indicates that objects reachable from those refs/tags
don't need to be stored in the upstream[^3].

When fetching from the recursive remote, we always download all its objects,
then ensure refs are valid. This means that a repository can successfully fetch
a rev from the upstream even if the upstream is missing some objects it depends
on, provided those objects are already present in the repository.

# Configuration

Recursive remotes are specified by prefixing the upstream repository with "recursive::". For example:

```
git remote add origin recursive::git@github.com:username/org.git
```

All configuration is done through Git's config system. The following configuration keys are available:

- `recursive-namespace`: Each branch on the remote repository can have multiple namespaces, each acting as an upstream for a separate repository. Unset is the same as empty string, aka "default namespace".
- `recursive-remote-branch`: The branch on the remote repository to use. Defaults to 'main'.
- `recursive-namespace-nacl-key`: The encryption key to use to encrypt this repository's contents on the remote.
- `recursive-state-nacl-key`: The encryption key to use to encrypt the branch metadata. All namespaces (repositories) on the same remote branch must use the same key.
- `recursive-shallow-basis`: Space-separated list of refs that don't need to be stored upstream. This is somewhat analogous to git shallow clone, though it is the upstream that is shallow instead of the local repository. This can be used to synchronize a repository across several machines that share large common history without needing to store the entire history upstream, but any new clones will need to get that common history via another mechanism such as an existing remote.
- `recursive-max-object-size`: Attempt to split objects stored upstream into chunks around this size.

## Encryption

- Encryption keys use [eseb](https://github.com/calmofthestorm/eseb), a thin wrapper around NaCl. They look similar to "eseb0::sym::jpjvT1mCbu3Am+m4F6SA2cGeY/ja6H+sAuK4Wy+zW/M=::31064"[^2].
- Each upstream branch is either completely unencrypted or encrypted.
- Repositories stored on an encrypted upstream branch:
  - Must specify the same `recursive-state-nacl-key`.
  - Must specify a value for `recursive-namespace-nacl-key`.
  - May use the same key as another repository, or `recursive-state-nacl-key`, for `recursive-namespace-nacl-key`.
- Setting any encryption key to the empty string, or a file that does not exist, will cause a random key to be generated and stored in the config/the specified file on first use.
- Encryption keys may be stored in a file with 'file://path/to/file'.
  - If the file does not exist, a random key will be generated and written to that path.
  - This is convenient if you want to commit the keys in the repository so that any clone can access the encrypted remote.
  - Keys may be generated explicitly using [eseb](https://github.com/calmofthestorm/eseb), or implicitly by pointing to a non-existent file or setting them to the empty string.

## Examples

### Default namespace, generate encryption keys on first use:

```
[remote "origin"]
    url = recursive::file:///home/username/recursive-upstream-repo
    fetch = +refs/heads/*:refs/remotes/origin/*
    recursive-remote-branch = main
    recursive-namespace = ""
    recursive-namespace-nacl-key = ""
    recursive-state-nacl-key = ""
```

### Namespace work, branch org, unencrypted

```
[remote "origin"]
    url = recursive::git@github.com:username/orgrepo.git
    fetch = +refs/heads/*:refs/remotes/origin/*
    recursive-remote-branch = org
    recursive-namespace = work
```

### Default namespace, use same key file for state and namespace

(generates keys on first use if file does not exist)

```
[remote "origin"]
    url = recursive::file:///home/username/recursive-upstream-repo
    fetch = +refs/heads/*:refs/remotes/origin/*
    recursive-remote-branch = main
    recursive-namespace = ""
    recursive-namespace-nacl-key = "file://.creds/recursive_remote_key"
    recursive-state-nacl-key = "file://.creds/recursive_remote_key"
```

# Known Issues

- The tracking repo fetches all branches from upstream, rather than just the one the current namespace is on. This is easy to fix, we just need to set the fetchspec properly.

# Implementation details

## Repository format

- Each branch on the upstream (backing) repository is completely independent. Recursive Remote operates on one branch at a time.
- Recursive Remote adds new commits to it, with the previous commit as parent, and does not force push.
- Recursive Remote does not assume it has exclusive access to the repository, and relies on git to prevent races on update.

## Commit/tree format

All files in the tree will be encrypted iff encryption is requested for the
remote. This section describes their decrypted contents. Many of these are
bincode-encoded structs from `serialization.rs`.

An important concept is that Recursive Remote essentially implements an object
graph on top of git's, using SHA256 instead of SHA1. Objects are retrieved from
git by their SHA1, but are also verified that their SHA256 matches.

In particular, the only time we actually traverse git's object graph is when
going from the branch to the SHA1 of `state.bincode` for that commit. This is a
weak spot that depends on SHA1 (with SHA256 TOFU at least). From then on, we
only traverse objects by the hashes stored in our own data structures.

This also means that we can use random names for objects stored in git, since we
neither enumerate git trees nor look up anything else by name. This avoids
leaking the name of namespaces and hashes of actual git packs.

Strictly speaking, the only reason we need to create a git tree at all is to
ensure all objects we need remain reachable so that git doesn't garbage collect
them. We'd also like to keep the tree consistent between commits to allow
efficient delta compression, which is somewhat at odds with the properties of
encryption. Potential future work could break up state more finely to improve
this.

Each commit's tree has a file called `state.bincode` at the root. This specifies
the current state of the branch, and can be thought of as the commit for our
object graph. It specifies a map from namespace name to the blob that represents
the state of that namespace, and the `state.bincode` of the parents of that commit.

There is also one tree (directory) per namespace at the root. Inside each tree
is `namespace.bincode`, which stores the refs for that namespace, and its packs.
Encrypted namespaces will also have a randomly generated name which is only used
when creating the git tree. Packs also have random names.

The packs subtree contains a directory structure where packs are stored
according to their hash (or random name if the repository is encrypted).

## Pack format

Packs stored in the repository are Git packs.

## Determining which objects to pack.

`git-pack-objects`, when used with --revs, accepts a set of commits to include
and a set to exclude, and packs the set difference. This lets us pack only the
objects not present on the other side. In a few cases we may duplicate an
object, but in general it is efficient.

## Pack synchronization (aka Packsos)

When injecting objects into the repository, we must ensure that all objects that
were reachable from the pushed refs on the pushing repository are present in the
pulling repository. To do this efficiently, we traverse the history graph, and
identify the set of all packs that may be required, as well as those we can
prove are covered by the objects in the repository (due to being sufficient to
recover a basis ref).

# (Possible) future work

- Use thin packs. Because we already guarantee all objects on the sender are
  present, this will be safe. This would help a lot with the size of incremental
  updates to large files.
- Improve automated test coverage.
  - Push semantics -- ensure non-ff in user repo are rejected.
  - Race condition when updating tracking repo.
  - Ratcheting.
  - Shallow basis.
  - Multiple namespaces on one branch.
    - Various combinations of same/shared encryption key.
- Extends feature for pack lists, to avoid namespace.bincode size being
  quadratic in commit count (since each pack must be mentioned in each commit).
  Alternatively, use the history instead of explicit extends.
- Basic read-only Git annex support, allowing a large repo to skip storing a few large packs in upstream.

# Bugs/Errata

- History traversal depends on being able to access the parent state.bincode
  going arbitrarily far back. We need to either keep that referenced or make it
  unnecessary. An alternative would be to fall back to re-inserting all packs if
  we ever encounter a broken link during the commit graph traversal.
- I may have found a bug where we can't fetch after pruning. Possibly the commit
  graph traversal algorithm is broken (aka, it's not safe to assume that we can
  terminate traversal at any commit where we have all refs and declare all its
  packs unnecessary)? It is also possible this specific case wase related to
  setup/surgery and won't recur. This can be worked around by forcing it to
  reinsert all packs.

# Footnotes

\[^1\]: It is possible to use per-repository keys with Gcrypt:

```
gpg --homedir .gnupg --full-gen-key
export KEY=<key>
git config gcrypt.participants $KEY
git config gcrypt.gpg-args "--homedir .gnupg"
```

\[^2\]: This example key is intentionally invalid to prevent accidental use.

\[^3\]: We decide what to send to the server using `git pack-objects --revs`. This
built-in command traverses the commit graph starting at all revs being
pushed, and terminating at any rev we know to be present on the remote, or
that is explicitly marked as a basis via the `recursive-shallow-basis`
config option. Thus, marking a rev as basis just pretends it exists on the
remote.

\[^4\]: I suppose some kind of UI could be added to require the user to specify
both SHA1 and SHA256 on the initial pull, but trust-on-first-use is good
enough for my threat model.
