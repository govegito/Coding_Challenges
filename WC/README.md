# Challenge 1: Build Your Own wc Tool

from
https://codingchallenges.fyi/challenges/challenge-wc/
by
https://twitter.com/johncrickett

## Usage
```bash
python ccwc.py [OPTIONS] [FILES]...
```
## Options
#### -l, --lines: Gets the number of lines in each input file.
#### -w, --words: Gets the number of words in each input file.
#### -c, --bytes: Gets the number of bytes in each input file.
#### -m, --char: Gets the number of characters in each input file.


### Count lines, words, and bytes in a file
```bash
python ccwc.py example.txt
```
### Count only lines in multiple files
```bash
python ccwc.py -l file1.txt file2.txt
```

### Count characters in standard input
```bash
echo "Hello, world!" | python ccwc.py -m
```

## Dependencies
Click: For command-line interface handling.
