#JDKPATH = /usr/lib/jvm/java-6-sun-1.6.0.22
JDKPATH = /usr/lib/jvm/java-1.6.0-openjdk
#LIBPATH = /home/arjun/dbi/minjava/javaminibase/src
LIBPATH = .:..  
#CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
