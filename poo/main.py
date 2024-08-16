import sys
# import transformations.transform as t
from transformations.transform import Animal


def main():
    animal = Animal()
    animal.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
