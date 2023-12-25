import sys


test_text = """2013-10-09\t13:22\tMiami\tBoots\t99.95\tVisa
2013-10-09\t13:22\tNewYork\tDVD\t9.50\tMasterCard
2013-10-0913:22:59I/OError
^d8x28orz28zoijzu1z1zp1OHH3du3ixwcz114<f
1\t2\t3"""


def main():
    from io import StringIO

    sys.stdin = StringIO(test_text)
    mapper()
    sys.stdin = sys.__stdin__


def mapper():
    for line in sys.stdin:
        data = line.strip().split("\t")
        if len(data) == 6:
            data, time, store, item, cost, payment = data
            print("{0}\t{1}".format(store, cost))


def reducer():
    salesTotal = 0
    oldKey = None

    for line in sys.stdin:
        data = line.strip().split("\t")
        if len(data) != 2:
            continue
        thisKey, thisSale = data

        if oldKey and oldKey != thisKey:
            print("{0}\t{1}".format(oldKey, salesTotal))
            salesTotal = 0

        oldKey = thisKey
        salesTotal += float(thisSale)

    if oldKey != None:
        print("{0}\t{1}".format(oldKey, salesTotal))


if __name__ == "__main__":
    main()
