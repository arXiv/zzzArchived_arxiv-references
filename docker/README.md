Docker setup
------------

We have not created daemons to run these containers as services. Therefore,
they require direct write permissions to the host disk mounted as a volume.  To
ensure that nothing bad happens, be sure to run the container as your current
user `-u $UID` and to only mount directories that can be completely destroyed
by a rogue process. There are additional controls that we should look into via
selinux that make fine-grained access rights for the container, but that is
down the road.

Building images
---------------

Each image can be built by running:

    docker build $DIRECTORY -t $NAME

Where NAME gives the image name as 'name:tag' format. For example, given the
domain 'arxiv-reflink', we may call these images 'arxiv-reflink/cermine:1.12'
and 'arxiv-reflink/autotex:0.906.0-1'. The DIRECTORY is the folder containing
each Dockerfile. See each directory for help in running the image and
information about the scripts etc.

External images
---------------

In addition to the images created here, we there are some images produced by
external organizations which we wish to use. In no particular order:

    * AI2 ScienceParse -- 
        docker run -p 8080:8080 --rm allenai-docker-public-docker.bintray.io/s2/scienceparse:1.2.8-SNAPSHOT

    * GROBID --
        docker run -t --rm -p 8080:8080 lfoppiano/grobid:0.4.1
