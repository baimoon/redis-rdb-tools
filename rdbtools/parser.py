import struct
import io
import sys
import datetime
import re

try :
    from StringIO import StringIO
except ImportError:
    from io import StringIO
    
REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 2
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255

REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3

DATA_TYPE_MAPPING = {
    0 : "string", 1 : "list", 2 : "set", 3 : "sortedset", 4 : "hash", 
    9 : "hash", 10 : "list", 11 : "set", 12 : "sortedset", 13 : "hash"}

class RdbCallback:
    """
    A Callback to handle events as the Redis dump file is parsed.
    This callback provides a serial and fast access to the dump file.
    
    """
    def start_rdb(self):
        """
        Called once we know we are dealing with a valid redis dump file
        
        """
        pass
        
    def start_database(self, db_number):
        """
        Called to indicate database the start of database `db_number` 
        
        Once a database starts, another database cannot start unless 
        the first one completes and then `end_database` method is called
        
        Typically, callbacks store the current database number in a class variable
        
        """     
        pass
    
    def set(self, key, value, expiry, info):
        """
        Callback to handle a key with a string value and an optional expiry
        
        `key` is the redis key
        `value` is a string or a number
        `expiry` is a datetime object. None and can be None
        `info` is a dictionary containing additional information about this object.
        
        """
        pass
    
    def start_hash(self, key, length, expiry, info):
        """Callback to handle the start of a hash
        
        `key` is the redis key
        `length` is the number of elements in this hash. 
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
        After that, the `end_hash` method will be called.
        
        """
        pass
    
    def hset(self, key, field, value):
        """
        Callback to insert a field=value pair in an existing hash
        
        `key` is the redis key for this hash
        `field` is a string
        `value` is the value to store for this field
        
        """
        pass
    
    def end_hash(self, key):
        """
        Called when there are no more elements in the hash
        
        `key` is the redis key for the hash
        
        """
        pass
    
    def start_set(self, key, cardinality, expiry, info):
        """
        Callback to handle the start of a hash
        
        `key` is the redis key
        `cardinality` is the number of elements in this set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
        After that, the `end_set` method will be called to indicate the end of the set.
        
        Note : This callback handles both Int Sets and Regular Sets
        
        """
        pass

    def sadd(self, key, member):
        """
        Callback to inser a new member to this set
        
        `key` is the redis key for this set
        `member` is the member to insert into this set
        
        """
        pass
    
    def end_set(self, key):
        """
        Called when there are no more elements in this set 
        
        `key` the redis key for this set
        
        """
        pass
    
    def start_list(self, key, length, expiry, info):
        """
        Callback to handle the start of a list
        
        `key` is the redis key for this list
        `length` is the number of elements in this list
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_list`, the method `rpush` will be called with `key` exactly `length` times
        After that, the `end_list` method will be called to indicate the end of the list
        
        Note : This callback handles both Zip Lists and Linked Lists.
        
        """
        pass
    
    def rpush(self, key, value) :
        """
        Callback to insert a new value into this list
        
        `key` is the redis key for this list
        `value` is the value to be inserted
        
        Elements must be inserted to the end (i.e. tail) of the existing list.
        
        """
        pass
    
    def end_list(self, key):
        """
        Called when there are no more elements in this list
        
        `key` the redis key for this list
        
        """
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        """
        Callback to handle the start of a sorted set
        
        `key` is the redis key for this sorted
        `length` is the number of elements in this sorted set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_sorted_set`, the method `zadd` will be called with `key` exactly `length` times. 
        Also, `zadd` will be called in a sorted order, so as to preserve the ordering of this sorted set.
        After that, the `end_sorted_set` method will be called to indicate the end of this sorted set
        
        Note : This callback handles sorted sets in that are stored as ziplists or skiplists
        
        """
        pass
    
    def zadd(self, key, score, member):
        """Callback to insert a new value into this sorted set
        
        `key` is the redis key for this sorted set
        `score` is the score for this `value`
        `value` is the element being inserted
        """
        pass
    
    def end_sorted_set(self, key):
        """
        Called when there are no more elements in this sorted set
        
        `key` is the redis key for this sorted set
        
        """
        pass
    
    def end_database(self, db_number):
        """
        Called when the current database ends
        
        After `end_database`, one of the methods are called - 
        1) `start_database` with a new database number
            OR
        2) `end_rdb` to indicate we have reached the end of the file
        
        """
        pass
    
    def end_rdb(self):
        """Called to indicate we have completed parsing of the dump file"""
        pass

class RdbParser :
    """
    A Parser for Redis RDB Files
    
    This class is similar in spirit to a SAX parser for XML files.
    The dump file is parsed sequentially. As and when objects are discovered,
    appropriate methods in the callback are called. 
        
    Typical usage :
        callback = MyRdbCallback() # Typically a subclass of RdbCallback
        parser = RdbParser(callback)
        parser.parse('/var/redis/6379/dump.rdb')
    
    filter is a dictionary with the following keys
        {"dbs" : [0, 1], "keys" : "foo.*", "types" : ["hash", "set", "sortedset", "list", "string"]}
        
        If filter is None, results will not be filtered
        If dbs, keys or types is None or Empty, no filtering will be done on that axis
    """
    def __init__(self, callback, filters = None) :
        """
            `callback` is the object that will receive parse events
        """
        self._callback = callback
        self._key = None
        self._expiry = None
        self.init_filter(filters)

    def parse(self, filename):
        """
        Parse a redis rdb dump file, and call methods in the 
        callback object during the parsing operation.
        """
        with open(filename, "rb") as f:
            #读取“REDIS”，如果不是该值，则报错
            self.verify_magic_string(f.read(5))
            #读取数据库的版本号"001--006"
            self.verify_version(f.read(4))
            self._callback.start_rdb()
            
            is_first_database = True
            db_number = 0
            while True :
                self._expiry = None
                #读取下一个无符号字符,系统的一些常量使用的都是用无符号的字符表示的
                data_type = read_unsigned_char(f)

                ####下面的if-else用于获取过期时间，最终获取的过期时间的单位是微秒，如果过期时间是毫秒，则按long类型读取，
                ####如果过期时间是秒，则按
                #判断是否是“过期时间（毫秒）”的标识
                if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS :
                    #读取并设置过期时间
                    self._expiry = to_datetime(read_unsigned_long(f) * 1000)
                    #读取下一个数据类型（无符号字符）
                    data_type = read_unsigned_char(f)
                #判断是否是“过期时间(秒)”的标识
                elif data_type == REDIS_RDB_OPCODE_EXPIRETIME :
                    self._expiry = to_datetime(read_unsigned_int(f) * 1000000)
                    data_type = read_unsigned_char(f)
                

                ####下面的if-else用户获取数据库选择，
                if data_type == REDIS_RDB_OPCODE_SELECTDB :
                    if not is_first_database :
                        self._callback.end_database(db_number)
                    is_first_database = False
                    db_number = self.read_length(f)
                    self._callback.start_database(db_number)
                    continue
                
                ####用于判断读取rdb文件是否结束
                if data_type == REDIS_RDB_OPCODE_EOF :
                    self._callback.end_database(db_number)
                    self._callback.end_rdb()
                    break

                ####判断数据库编号(db_number)是否在类的dbs中
                if self.matches_filter(db_number) :
                    #读取key信息，key肯定是字符串
                    self._key = self.read_string(f)
                    if self.matches_filter(db_number, self._key, data_type):
                        self.read_object(f, data_type)
                    else:
                        self.skip_object(f, data_type)
                else :
                    self.skip_key_and_object(f, data_type)

    ####*****************************
    ####当读取的字符是db_number时，判断长度：0-63使用1个字节表示， 64-16383使用两个字节表示，16383-2^32-1使用5个直接表示
    ####*****************************
    def read_length_with_encoding(self, f) :
        length = 0
        is_encoded = False
        bytes = []
        #读取无符号字符：return struct.unpack('B', f.read(1))[0]
        bytes.append(read_unsigned_char(f))
        ####bytes[0] & 11000000 >> 6----就是用来获取bytes[0]的前两位，判断这两位的数值，可能是：01, 11, 10, 00
        enc_type = (bytes[0] & 0xC0) >> 6
        
        #如果enc_type为 11==3， 则表示剩下的六位是一个特殊字符
        ####由此处和read_string方法可以看出如果enc_type==3，那么后六位的值只可能是以下4种：00, 01, 10, 11
        ####00代表：要读取的值为带符号的一个字节
        ####01代表：要读取的值为带符号的两个字节
        ####10代表：要读取的值为带符号的四个字节
        ####11代表：要读取的数据是LZF压缩后的。压缩的格式遵循 compress_lenth origin_lenth str
        if enc_type == REDIS_RDB_ENCVAL :
            is_encoded = True
            #0x3F的二进制表示为：111111，下面的操作是为了获取后6位
            length = bytes[0] & 0x3F
        #如果enc_type是 00==0，则表示剩下的六位是具体的长度
        elif enc_type == REDIS_RDB_6BITLEN :
            #0x3F的二进制表示为：111111，下面的操作是为了获取后6位
            length = bytes[0] & 0x3F
        #如果enc_type是 01==1， 则表示再读取1个字节，加上前面的6位，一共14位表示具体的长度
        elif enc_type == REDIS_RDB_14BITLEN :
            #读取流中的下一位
            bytes.append(read_unsigned_char(f))
            #0x3F的二进制表示为：111111，和bytes[0]进行按位与，获取byte[0]的后6为，左移8位与byte[1]组成一个14位的数值
            length = ((bytes[0]&0x3F)<<8)|bytes[1]
        #如果enc_type是10==2， 则表示剩下六位废弃，再读取后面的4个字节，作为长度
        else :
            length = ntohl(f)
        return (length, is_encoded)

    def read_length(self, f) :
        return self.read_length_with_encoding(f)[0]

    ####读取rdb文件中的字符串
    ####字符串分为非压缩和压缩两种存储方式
    ####非压缩字符串的结构：lenth  val
    ####压缩字符串的结构：  REDIS_RDB_ENC_LZF  compress_lenth  origin_lenth val
    def read_string(self, f) :
        #tup是一个包含lenth和is_encoded的元组
        tup = self.read_length_with_encoding(f)
        #获取长度和是否编码
        length = tup[0]
        is_encoded = tup[1]
        val = None
        #is_encoded用来表示是否进行了编码处理
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                val = read_signed_char(f)
            elif length == REDIS_RDB_ENC_INT16 :
                val = read_signed_short(f)
            elif length == REDIS_RDB_ENC_INT32 :
                val = read_signed_int(f)
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                val = self.lzf_decompress(f.read(clen), l)
        #没有进行编码处理的数据，可以直接读取
        else :
            val = f.read(length)
        return val

    # Read an object for the stream
    # f is the redis file 
    # enc_type is the type of object
    ####读取对象，f是流， enc_type是对象的类型，对象的类型应该是
    def read_object(self, f, enc_type) :
        #字符串类型
        if enc_type == REDIS_RDB_TYPE_STRING :
            val = self.read_string(f)
            self._callback.set(self._key, val, self._expiry, info={'encoding':'string'})
        #list类型
        elif enc_type == REDIS_RDB_TYPE_LIST :
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a list from it
            # The lists are in order i.e. the first string is the head, 
            # and the last string is the tail of the list
            ####---------------------LIST的结构-------------------
            ####| lenth  |  item1  |  item2  |  ...  |  item N  |
            ####-------------------------------------------------
            length = self.read_length(f)
            self._callback.start_list(self._key, length, self._expiry, info={'encoding':'linkedlist' })
            for count in xrange(0, length) :
                val = self.read_string(f)
                self._callback.rpush(self._key, val)
            self._callback.end_list(self._key)
        #set类型
        elif enc_type == REDIS_RDB_TYPE_SET :
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a set from it
            # Note that the order of strings is non-deterministic
            length = self.read_length(f)
            self._callback.start_set(self._key, length, self._expiry, info={'encoding':'hashtable'})
            for count in xrange(0, length) :
                val = self.read_string(f)
                self._callback.sadd(self._key, val)
            self._callback.end_set(self._key)
        #zset类型
        elif enc_type == REDIS_RDB_TYPE_ZSET :
            length = self.read_length(f)
            self._callback.start_sorted_set(self._key, length, self._expiry, info={'encoding':'skiplist'})
            for count in xrange(0, length) :
                val = self.read_string(f)
                dbl_length = read_unsigned_char(f)
                score = f.read(dbl_length)
                if isinstance(score, str):
                    score = float(score)
                self._callback.zadd(self._key, score, val)
            self._callback.end_sorted_set(self._key)
        #hash类型
        elif enc_type == REDIS_RDB_TYPE_HASH :
            length = self.read_length(f)
            self._callback.start_hash(self._key, length, self._expiry, info={'encoding':'hashtable'})
            for count in xrange(0, length) :
                field = self.read_string(f)
                value = self.read_string(f)
                self._callback.hset(self._key, field, value)
            self._callback.end_hash(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            self.read_zipmap(f)
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            self.read_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            self.read_intset(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            self.read_zset_from_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            self.read_hash_from_ziplist(f)
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))

    def skip_key_and_object(self, f, data_type):
        self.skip_string(f)
        self.skip_object(f, data_type)

    def skip_string(self, f):
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        bytes_to_skip = 0
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                bytes_to_skip = 1
            elif length == REDIS_RDB_ENC_INT16 :
                bytes_to_skip = 2
            elif length == REDIS_RDB_ENC_INT32 :
                bytes_to_skip = 4
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                bytes_to_skip = clen
        else :
            bytes_to_skip = length
        
        skip(f, bytes_to_skip)

    def skip_object(self, f, enc_type):
        skip_strings = 0
        if enc_type == REDIS_RDB_TYPE_STRING :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_SET :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            skip_strings = 1
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in xrange(0, skip_strings):
            self.skip_string(f)


    def read_intset(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        encoding = read_unsigned_int(buff)
        num_entries = read_unsigned_int(buff)
        self._callback.start_set(self._key, num_entries, self._expiry, info={'encoding':'intset', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            if encoding == 8 :
                entry = read_unsigned_long(buff)
            elif encoding == 4 :
                entry = read_unsigned_int(buff)
            elif encoding == 2 :
                entry = read_unsigned_short(buff)
            else :
                raise Exception('read_intset', 'Invalid encoding %d for key %s' % (encoding, self._key))
            self._callback.sadd(self._key, entry)
        self._callback.end_set(self._key)

    def read_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        self._callback.start_list(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            val = self.read_ziplist_entry(buff)
            self._callback.rpush(self._key, val)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_list(self._key)

    def read_zset_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_zset_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries /2
        self._callback.start_sorted_set(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            member = self.read_ziplist_entry(buff)
            score = self.read_ziplist_entry(buff)
            if isinstance(score, str) :
                score = float(score)
            self._callback.zadd(self._key, score, member)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_zset_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_sorted_set(self._key)

    def read_hash_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_hash_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries /2
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            field = self.read_ziplist_entry(buff)
            value = self.read_ziplist_entry(buff)
            self._callback.hset(self._key, field, value)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_hash_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_hash(self._key)
    
    
    def read_ziplist_entry(self, f) :
        length = 0
        value = None
        prev_length = read_unsigned_char(f)
        if prev_length == 254 :
            prev_length = read_unsigned_int(f)
        entry_header = read_unsigned_char(f)
        if (entry_header >> 6) == 0 :
            length = entry_header & 0x3F
            value = f.read(length)
        elif (entry_header >> 6) == 1 :
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)
            value = f.read(length)
        elif (entry_header >> 6) == 2 :
            length = read_big_endian_unsigned_int(f)
            value = f.read(length)
        elif (entry_header >> 4) == 12 :
            value = read_signed_short(f)
        elif (entry_header >> 4) == 13 :
            value = read_signed_int(f)
        elif (entry_header >> 4) == 14 :
            value = read_signed_long(f)
        elif (entry_header == 240) :
            value = read_24bit_signed_number(f)
        elif (entry_header == 254) :
            value = read_signed_char(f)
        elif (entry_header >= 241 and entry_header <= 253) :
            value = entry_header - 241
        else :
            raise Exception('read_ziplist_entry', 'Invalid entry_header %d for key %s' % (entry_header, self._key))
        return value
        
    def read_zipmap(self, f) :
        raw_string = self.read_string(f)
        buff = io.BytesIO(bytearray(raw_string))
        num_entries = read_unsigned_char(buff)
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'zipmap', 'sizeof_value':len(raw_string)})
        while True :
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                break
            key = buff.read(next_length)
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                raise Exception('read_zip_map', 'Unexepcted end of zip map for key %s' % self._key)        
            free = read_unsigned_char(buff)
            value = buff.read(next_length)
            try:
                value = int(value)
            except ValueError:
                pass
            
            skip(buff, free)
            self._callback.hset(self._key, key, value)
        self._callback.end_hash(self._key)

    def read_zipmap_next_length(self, f) :
        num = read_unsigned_char(f)
        if num < 254:
            return num
        elif num == 254:
            return read_unsigned_int(f)
        else:
            return None

    def verify_magic_string(self, magic_string) :
        if magic_string != 'REDIS' :
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str) :
        version = int(version_str)
        if version < 1 or version > 6 : 
            raise Exception('verify_version', 'Invalid RDB version number %d' % version)

    def init_filter(self, filters):
        self._filters = {}
        if not filters:
            filters={}

        if not 'dbs' in filters:
            self._filters['dbs'] = None
        elif isinstance(filters['dbs'], int):
            self._filters['dbs'] = (filters['dbs'], )
        elif isinstance(filters['dbs'], list):
            self._filters['dbs'] = [int(x) for x in filters['dbs']]
        else:
            raise Exception('init_filter', 'invalid value for dbs in filter %s' %filters['dbs'])
        
        if not ('keys' in filters and filters['keys']):
            self._filters['keys'] = re.compile(".*")
        else:
            self._filters['keys'] = re.compile(filters['keys'])

        if not 'types' in filters:
            self._filters['types'] = ('set', 'hash', 'sortedset', 'string', 'list')
        elif isinstance(filters['types'], str):
            self._filters['types'] = (filters['types'], )
        elif isinstance(filters['types'], list):
            self._filters['types'] = [str(x) for x in filters['types']]
        else:
            raise Exception('init_filter', 'invalid value for types in filter %s' %filters['types'])
    ####匹配过滤器--
    ####要求db_number在类的dbs中，key在类的keys中，data_type在类的types中
    ####
    def matches_filter(self, db_number, key=None, data_type=None):
        ##如果存在dbs，并且db_number不存在于dbs中，则返回false
        if self._filters['dbs'] and (not db_number in self._filters['dbs']):
            return False
        ##如果key不为None，并且keys中不存在key，则返回false
        if key and (not self._filters['keys'].match(str(key))):
            return False
        ##如果data_type不为None，并且types不存在于data_type中，则返回false
        if data_type is not None and (not self.get_logical_type(data_type) in self._filters['types']):
            return False
        return True
    
    def get_logical_type(self, data_type):
        return DATA_TYPE_MAPPING[data_type]
        
    def lzf_decompress(self, compressed, expected_length):
        in_stream = bytearray(compressed)
        in_len = len(in_stream)
        in_index = 0
        out_stream = bytearray()
        out_index = 0
    
        while in_index < in_len :
            ctrl = in_stream[in_index]
            if not isinstance(ctrl, int) :
                raise Exception('lzf_decompress', 'ctrl should be a number %s for key %s' % (str(ctrl), self._key))
            in_index = in_index + 1
            if ctrl < 32 :
                for x in xrange(0, ctrl + 1) :
                    out_stream.append(in_stream[in_index])
                    #sys.stdout.write(chr(in_stream[in_index]))
                    in_index = in_index + 1
                    out_index = out_index + 1
            else :
                length = ctrl >> 5
                if length == 7 :
                    length = length + in_stream[in_index]
                    in_index = in_index + 1
                
                ref = out_index - ((ctrl & 0x1f) << 8) - in_stream[in_index] - 1
                in_index = in_index + 1
                for x in xrange(0, length + 2) :
                    out_stream.append(out_stream[ref])
                    ref = ref + 1
                    out_index = out_index + 1
        if len(out_stream) != expected_length :
            raise Exception('lzf_decompress', 'Expected lengths do not match %d != %d for key %s' % (len(out_stream), expected_length, self._key))
        return str(out_stream)

def skip(f, free):
    if free :
        f.read(free)

def ntohl(f) :
    #读取流中后面4位
    val = read_unsigned_int(f)
    new_val = 0
    new_val = new_val | ((val & 0x000000ff) << 24)
    new_val = new_val | ((val & 0xff000000) >> 24)
    new_val = new_val | ((val & 0x0000ff00) << 8)
    new_val = new_val | ((val & 0x00ff0000) >> 8)
    return new_val

def to_datetime(usecs_since_epoch):
    seconds_since_epoch = usecs_since_epoch / 1000000
    useconds = usecs_since_epoch % 1000000
    dt = datetime.datetime.utcfromtimestamp(seconds_since_epoch)
    delta = datetime.timedelta(microseconds = useconds)
    return dt + delta
    
def read_signed_char(f) :
    return struct.unpack('b', f.read(1))[0]
    
def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]

def read_signed_short(f) :
    return struct.unpack('h', f.read(2))[0]
        
def read_unsigned_short(f) :
    return struct.unpack('H', f.read(2))[0]

def read_signed_int(f) :
    return struct.unpack('i', f.read(4))[0]
    
def read_unsigned_int(f) :
    return struct.unpack('I', f.read(4))[0]

def read_big_endian_unsigned_int(f):
    return struct.unpack('>I', f.read(4))[0]

def read_24bit_signed_number(f):
    s = '0' + f.read(3)
    num = struct.unpack('i', s)[0]
    return num >> 8
    
def read_signed_long(f) :
    return struct.unpack('q', f.read(8))[0]
    
def read_unsigned_long(f) :
    return struct.unpack('Q', f.read(8))[0]

def string_as_hexcode(string) :
    for s in string :
        if isinstance(s, int) :
            print(hex(s))
        else :
            print(hex(ord(s)))


class DebugCallback(RdbCallback) :
    def start_rdb(self):
        print('[')
    
    def start_database(self, db_number):
        print('{')
    
    def set(self, key, value, expiry):
        print('"%s" : "%s"' % (str(key), str(value)))
    
    def start_hash(self, key, length, expiry):
        print('"%s" : {' % str(key))
        pass
    
    def hset(self, key, field, value):
        print('"%s" : "%s"' % (str(field), str(value)))
    
    def end_hash(self, key):
        print('}')
    
    def start_set(self, key, cardinality, expiry):
        print('"%s" : [' % str(key))

    def sadd(self, key, member):
        print('"%s"' % str(member))
    
    def end_set(self, key):
        print(']')
    
    def start_list(self, key, length, expiry):
        print('"%s" : [' % str(key))
    
    def rpush(self, key, value) :
        print('"%s"' % str(value))
    
    def end_list(self, key):
        print(']')
    
    def start_sorted_set(self, key, length, expiry):
        print('"%s" : {' % str(key))
    
    def zadd(self, key, score, member):
        print('"%s" : "%s"' % (str(member), str(score)))
    
    def end_sorted_set(self, key):
        print('}')
    
    def end_database(self, db_number):
        print('}')
    
    def end_rdb(self):
        print(']')


