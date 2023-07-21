
libbraft_compat.so: braft_compat.cpp
	g++ -shared  -fPIC  -DGFLAGS_NS=gflags -Wall -lbrpc -lbraft -lgflags braft_compat.cpp -o $@
