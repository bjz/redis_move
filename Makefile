redis_move: main.o redis_move.o redis_move.hpp
	g++ -o redis_move main.o redis_move.o redis_move.hpp -static -lhiredis -I/usr/local/include/hiredis -pthread -g -lm -L/usr/lib/x86_64-redhat-linux5E/lib64/

main.o: main.cpp 
	g++ -c main.cpp -I/usr/local/include/hiredis

redis_move.o: redis_move.cpp 
	g++ -c redis_move.cpp  -I/usr/local/include/hiredis

clean:
	rm -rf *.o
