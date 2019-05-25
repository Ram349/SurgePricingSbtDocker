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
    
	stage('Docker Publish') {
	
			sh ${tool name: 'sbt', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt docker:publishLocal"
		
	}
    
}