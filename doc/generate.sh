#!/bin/bash

DOC_TARGET=target/doc

if [[ ! -d $DOC_TARGET ]]; then
    GIT_REMOTE=$(git remote -v | head -1 | awk '{ print $2; }')
    echo "Cloning gh-pages branch from $GIT_REMOTE into $DOC_TARGET ..."
    mkdir -p $(dirname $DOC_TARGET) || exit 2
    git clone $GIT_REMOTE $DOC_TARGET || exit 2
    pushd $DOC_TARGET
    git checkout gh-pages || exit 3
    if [[ $1 == "init" ]]; then
        git symbolic-ref HEAD refs/heads/gh-pages
        rm .git/index
        git clean -fdx
    fi
    popd
fi

echo "Generating documentation in $DOC_TARGET ..."
lein with-profile +doc do codox, marg --dir $DOC_TARGET/marginalia
