import sys
import click

def count_all(file_data):
    line_count=char_count=word_count=byte_count=0
    for line in file_data:
        line_count+=1
        byte_count+=len(line)
        word_count+=len(line.split())
        char_count+=len(line.decode())
    return line_count,char_count,word_count,byte_count

@click.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option(
    "-l",
    "--lines",
    "count_lines",
    is_flag=True,
    help="Gets the number of lines in each input file",
)
@click.option(
    "-w",
    "--words",
    "count_words",
    is_flag=True,
    help="Gets the number of words in each input file",
)
@click.option(
    "-c",
    "--bytes",
    "count_bytes",
    is_flag=True,
    help="Gets the number of bytes in each input file",
)
@click.option(
    "-m",
    "--char",
    "count_char",
    is_flag=True,
    help="Gets the number of characters in each input file",
)
def get_count(files,count_lines,count_words,count_bytes,count_char):
    if count_char:
        pass
    elif not(count_lines or count_words or count_bytes):
        count_lines=count_words=count_bytes=True

    if not files:
        files=["-"]

    for file in files:
        if file=="-":
            content=sys.stdin.buffer
            result=count_all(content)
        else:
            with open(file,"rb") as content:
                result=count_all(content)
        line_count,char_count,word_count,byte_count=result

        if not (count_lines or count_char or count_words or count_bytes):
            result=f"{line_count}\t{word_count}\t{byte_count}\n"
        else:
            result=f"""{str(line_count)+' ' if count_lines else ''}{str(word_count)+' ' if count_words else ''}{str(byte_count)+' ' if count_bytes else ''}{str(char_count)+' ' if count_char else ''}{file}\n"""

        click.echo(result)

if __name__ == '__main__':
    get_count()