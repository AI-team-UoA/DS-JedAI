FROM ubuntu:18.04
LABEL image=DS-JEDAI-docker
ENV SPARK_VERSION=2.4.4
ENV HADOOP_VERSION=2.7
ENV SCALA_VERSION=2.11.12
ENV SBT_VERSION=1.3.10

# Install gnu, wget, java and git
RUN apt-get update -qq && \
     apt-get install -qq -y gnupg2 wget openjdk-8-jdk git && \
     java -version && \
     git --version

# install scala
RUN wget  --no-verbose http://scala-lang.org/files/archive/scala-$SCALA_VERSION.deb && \
    dpkg -i scala-$SCALA_VERSION.deb && \
    apt-get update -qq && \
    apt-get install -qq -y scala && \
    scala -version

# install sbt
RUN wget --no-verbose -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
      dpkg -i sbt-$SBT_VERSION.deb && \
      rm sbt-$SBT_VERSION.deb && \
      apt-get update -qq && \
      apt-get install -qq -y sbt && \
      sbt sbtVersion

#Download the Spark binaries from the repo
WORKDIR /
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrcc && \
    spark-submit --version

#Expose the UI Port 4040
EXPOSE 4040

RUN git clone https://github.com/GiorgosMandi/DS-JedAI.git
RUN cd DS-JedAI && sbt assebly
