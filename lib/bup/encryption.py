import os

from helpers import *
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers import (
    Cipher, algorithms, modes
)
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac

def encrypt(key, plaintext, associated_data):
    # Generate a random 96-bit IV.
    iv = os.urandom(12)

    # Construct an AES-GCM Cipher object with the given key and a
    # randomly generated IV.
    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv),
        backend=default_backend()
    ).encryptor()

    # associated_data will be authenticated but not encrypted,
    # it must also be passed in on decryption.
    encryptor.authenticate_additional_data(associated_data)

    # Encrypt the plaintext and get the associated ciphertext.
    # GCM does not require padding.
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()

    return (iv, ciphertext, encryptor.tag)

def decrypt(key, associated_data, iv, ciphertext, tag):
    # Construct a Cipher object, with the key, iv, and additionally the
    # GCM tag used for authenticating the message.
    decryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv, tag),
        backend=default_backend()
    ).decryptor()

    # We put associated_data back in or the tag will fail to verify
    # when we finalize the decryptor.
    decryptor.authenticate_additional_data(associated_data)

    # Decryption gets us the authenticated plaintext.
    # If the tag does not match an InvalidTag exception will be raised.
    return decryptor.update(ciphertext) + decryptor.finalize()

# iv, ciphertext, tag = encrypt(
#     key,
#     b"a secret message!",
#     b"authenticated but not encrypted payload"
# )

# print(decrypt(
#     key,
#     b"authenticated but not encrypted payload",
#     iv,
#     ciphertext,
#     tag
# ))




m = b"A really secret message. Not for prying eyes."
print (m)
print ("tam: ",len(m)) 
key = Fernet.generate_key()
cipher_suite = Fernet(key)
cipher_text = cipher_suite.encrypt(m)
print (cipher_text)
print ("tam: ",len(cipher_text)) 
plain_text = cipher_suite.decrypt(cipher_text)
print (plain_text)


#print key
#h = hmac.HMAC(key, hashes.SHA256(), backend=default_backend())
#h.update(b"message to hash")
#m = h.finalize()
#print m
#h = hmac.HMAC(key, hashes.SHA256(), backend=default_backend())
#print h.verify(m)

#err
#h.verify(b"an incorrect signature")
