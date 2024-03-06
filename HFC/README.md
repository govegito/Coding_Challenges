# Build Your Own Compression Tool

from https://codingchallenges.fyi/challenges/challenge-huffman

So before starting this challenge. I went throught this article for getting to know the [Huffman encoding](https://opendsa-server.cs.vt.edu/ODSA/Books/CS3/html/Huffman.html) 
and [this](https://www.joelonsoftware.com/2003/10/08/the-absolute-minimum-every-software-developer-absolutely-positively-must-know-about-unicode-and-character-sets-no-excuses/) article about 
unicode and text encodings.

## Usage

### encoding
```bash
java HFC -e -i [input_file_path] -o [output_file_path]
```

### decoding
```bash
java HFC -d -i [input_file_path] -o [output_file_path]
```

### testing both
```bash
java HFS -t -i [input_file_path] -o [encoded_file_path] -f [decoded_file_path]
```

