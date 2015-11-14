Github Page Docs
================

Library documentation is served as a static site by Github Pages in the
`gh-pages` branch. Documentation should be updated after every release to
`master`.

To set up a subdirectory with the documentation branch, use the following
commands in a fresh repo:

```bash
cd $REPO
git clone $(git remote -v | head -1 | awk '{ print $2; }') doc
cd doc
git symbolic-ref HEAD refs/heads/gh-pages
rm .git/index
git clean -fdx
```

After this, you can regenerate the documentation by running:

```bash
lein do codox, doc-lit
```
