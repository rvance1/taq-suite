import lz4.frame

def hexdump_taq(bin_path, num_bytes=160):
    with lz4.frame.open(bin_path, 'rb') as f:
        raw = f.read(num_bytes)
        
    print("--- RAW HEX DUMP (TICKER 'A' - BYTE 0) ---")
    for i in range(0, len(raw), 16):
        chunk = raw[i:i+16]
        # Format as Hex
        hex_str = " ".join([f"{b:02X}" for b in chunk])
        # Format as ASCII (dots for non-printable characters)
        ascii_str = "".join([chr(b) if 32 <= b <= 126 else "." for b in chunk])
        print(f"{i:04X}  {hex_str:<47}  |{ascii_str}|")

hexdump_taq('data/raw/taq/taq1999/CQ9901A.BIN.lz4')