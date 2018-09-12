#!/bin/bash
set -e

function lint {
    dir=$1
    (
        cd "$dir"
        source venv/bin/activate
        pyflakes db_manager/
        pylint -E db_manager/
    )
}

lint db-manager
