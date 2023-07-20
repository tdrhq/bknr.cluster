
libbraft_compat.so: braft_compat.cpp
	g++ -shared  -fPIC  -DGFLAGS_NS=gflags -lbrpc -lbraft -lgflags braft_compat.cpp -o $@
