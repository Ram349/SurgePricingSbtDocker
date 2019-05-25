node {
    def app

    stage('Compile') {
    
			echo "Compiling..."
			sh "${tool name: 'sbt', type:'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile"
		
		
	}
	stage('Build') {
    
			echo "Assemblying..."
			sh "${tool name: 'sbt', type:'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt assembly"
		
		
	}
    
	stage('Build image') {
        /* This builds the actual image; synonymous to
         * docker build on the command line */

        app = docker.build("rampradhan/surgepricesbtdocker")
    }

    stage('Test image') {
        /* Ideally, we would run a test framework against our image.
         * For this example, we're using a Volkswagen-type approach ;-) */

        app.inside {
            
        }
    }

    stage('Push image') {
        /* Finally, we'll push the image with two tags:
         * First, the incremental build number from Jenkins
         * Second, the 'latest' tag.
         * Pushing multiple tags is cheap, as all the layers are reused. */
        docker.withRegistry('https://registry.hub.docker.com', 'docker-hub-credentials') {
            app.push("${env.BUILD_NUMBER}")
            app.push("latest")
        }
    }
    
}