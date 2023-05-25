import hashlib
import binascii
import base58

def Ripe160HashtoBase58(ripemd160hash):
        d = int('00' + ripemd160hash+ hashlib.sha256(hashlib.sha256(binascii.unhexlify('00' + ripemd160hash)).digest()).hexdigest()[0:8],16)
        return '1' + str(base58.b58encode(d.to_bytes((d.bit_length() + 7) // 8, 'big')))[2:-1]

