node {
    def app

    stage('Compile') {
    steps {
			echo "Compiling..."
			sh "${tool name: 'sbt', type:'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt compile"
		}
		
	}
	stage('Build') {
    steps {
			echo "Assemblying..."
			sh "${tool name: 'sbt', type:'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt assembly"
		}
		
	}
    
	stage('Docker Publish') {
	steps {
			sh ${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt docker:publishLocal"
		}
	}
    
}