pipeline {
	agent any
	
	environment {
		BuildConfiguration="Debug"
	}

	stages {
		stage('Build') {
			steps {
				sh "dotnet build -c $BuildConfiguration"
			}
		}

		stage('Test') {
			steps {
				sh 'dotnet test -c $BuildConfiguration -l trx --no-build'
			}
		}
		
		stage('Pack & Push Tgs.MessageQueue') {
			steps {
				sh 'dotnet pack Tgs.MessageQueue/Tgs.MessageQueue.csproj -c $BuildConfiguration --no-build --no-restore'
				sh 'dotnet nuget push Tgs.MessageQueue/bin/$BuildConfiguration/*.nupkg --source "raypro" --skip-duplicate'
			}
		}

        stage('Pack & Push Tgs.MessageQueue.Nats') {
			steps {
				sh 'dotnet pack Tgs.MessageQueue.Nats/Tgs.MessageQueue.Nats.csproj -c $BuildConfiguration --no-build --no-restore'
				sh 'dotnet nuget push Tgs.MessageQueue.Nats/bin/$BuildConfiguration/*.nupkg --source "raypro" --skip-duplicate'
			}
		}

		stage('Pack & Push Tgs.MessageQueue.RabbitMQ') {
			steps {
				sh 'dotnet pack Tgs.MessageQueue.RabbitMQ/Tgs.MessageQueue.RabbitMQ.csproj -c $BuildConfiguration --no-build --no-restore'
				sh 'dotnet nuget push Tgs.MessageQueue.RabbitMQ/bin/$BuildConfiguration/*.nupkg --source "raypro-baget" -k admin --skip-duplicate'
			}
		}
	}

	post {
        always {
            step([$class: 'MSTestPublisher', testResultsFile:"**/*.trx", failOnError: false, keepLongStdio: true])
        }
    }
}
