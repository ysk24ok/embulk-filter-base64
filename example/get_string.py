import sys
import base64

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit("""Invalid number of command-line arguments.
Usage: python {} [decode/encode] [str]""".format(__file__))
    base64_order = sys.argv[1]
    target_str = sys.argv[2]
    if base64_order == 'decode':
        print(base64.decodestring(target_str))
    elif base64_order == 'encode':
        print(base64.encodestring(target_str).strip())
    else:
        sys.exit("Invalid base64_order. Specify 'decode' or 'encode'.")
