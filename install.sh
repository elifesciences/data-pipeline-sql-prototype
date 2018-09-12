#!/bin/bash
# why not just use pipenv directly?
# pipenv makes it quite difficult to specify where to store it's state
# pipenv seems immature
set -e

function install {
    dir=$1
    (
        cd "$dir"
        echo "$dir"
        python3.5 -m venv venv
        source venv/bin/activate
        pip install pipenv
        pipenv install
    )
}

install db-manager
install csv-generator
