## MSE KVDB Storage Engine Module for MongoDB

### TL;DR

Execute this series of commands to compile and run MongoDB with MSE KVDB storage engine:

    # get mongodb
    git clone -b mse-mongo-3.4.7  ssh://git@bitbucket.micron.com/sbusw/mongo.git
    # get mongo-mse-kvdb
    git clone -b mse-mongo-3.4.7 ssh://git@bitbucket.micron.com/sbusw/mongo-mse-kvdb.git
    # add mongo-mse-kvdb module to mongo
    mkdir -p mongo/src/mongo/db/modules/
    ln -sf $(pwd)/mongo-mse-kvdb mongo/src/mongo/db/modules/mongo-mse-kvdb
	# get MSE
	git clone -b nfpib ssh://git@bitbucket.micron.com/sbusw/nf.git
	# compile MSE, change target relwithdebug to debug or release as needed...
	cd nf && make clean relwithdebug && make config relwithdebug && make package relwithdebug && cd ..
	# install MSE RPMS
	sudo dnf install nf/builds/relwithdebug/*.rpm
    # compile mongo
    cd mongo; scons
	# create and mount an mpool on a device such as /dev/sdb
	/bin/nf pd prepare /dev/<devname>
	# create mpool on a partition
	/bin/nf mpool create mp1 /dev/<devname>1
	# mount mpool
	/bin/nf mpool mount mp1

Start `mongod` using the `--storageEngine=mse-kvdb` option.
e.g.,
    mongod --storageEngine=mse-kvdb --dbpath=path_to_dbdir
    
