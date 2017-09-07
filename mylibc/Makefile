CC = gcc
CFLAGS = -O2 -fPIC -Wall
LUADIR = /usr/maidong/skynet/3rd/lua
TARGET = package.so
SKYNETDIR = /usr/maidong/skynet/skynet-src/

.PHONY : all clean

all : $(TARGET)

$(TARGET) : package_proto.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR)  $^
	cp $(TARGET) /usr/maidong/skynet/luaclib/

clean :
	rm -f $(TARGET)
