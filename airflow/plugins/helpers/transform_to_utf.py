#!/usr/bin/env python3

import sys
import subprocess


def main(input_file, output_file):
    iconv = 'iconv -f CP1252 -t UTF-8 {}'.format(input_file)

    with open(output_file, 'w') as f:
        subprocess.call(iconv.split(), stdout=f)


if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    main(input_file, output_file)
