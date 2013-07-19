(defproject storm-starter "0.0.1-SNAPSHOT"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :dependencies []
  :dev-dependencies [
                     [storm "0.7.2"]
                     ])