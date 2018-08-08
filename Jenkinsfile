elifePipeline {
    def commit
    DockerImage image
    stage 'Checkout', {
        checkout scm
        commit = elifeGitRevision()
    }

    node('containers-jenkins-plugin') {
        stage 'Build images', {
            checkout scm
            dockerComposeBuild(commit)
        }

        stage 'Project tests', {
            try {
                sh "IMAGE_TAG=${commit} docker-compose run --rm --entrypoint 'sh -c' csv-generator ./project_tests.sh"
                sh "IMAGE_TAG=${commit} docker-compose run --rm db-manager ./project_tests.sh"
                sh "IMAGE_TAG=${commit} docker-compose run --rm airflow ./project_tests.sh"
            } finally {
                sh 'docker-compose down -v'
            }
        }

        elifeMainlineOnly {
            stage 'Push image', {
                image = DockerImage.elifesciences(this, "data-pipeline-airflow", commit)
                image.push()
            }
        }
    }

    elifeMainlineOnly {
        stage 'Approval', {
            elifeGitMoveToBranch commit, 'approved'
            node('containers-jenkins-plugin') {
                image = DockerImage.elifesciences(this, "data-pipeline-airflow", commit)
                image.pull().tag('approved').push()
            }
        }
    }
}

