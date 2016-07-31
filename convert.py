
lineId = '1'


def itemset_format(eid, itemset):
    return eid + ':' + ",".join(itemset)


def build_seq(sid, sequence):
    result = []
    if len(sequence) == 0:
        return ';'.join(result)

    eid = sequence[0][0]
    itemset = []

    for item in sequence:
        if item[0] == eid:
            itemset.append(item[1])
        else:
            result.append(itemset_format(eid, itemset))
            itemset = []
            eid = item[0]
            itemset.append(item[1])
    result.append(itemset_format(eid, itemset))
    return ';'.join(result)

if __name__ == '__main__':

    f = open('format.txt', 'w')
    seq = []
    for line in open('testData.txt', 'r'):
        words = line.rstrip('\n').split(' ')
        if words[0] == lineId:
            seq.append(words[1:3])
        else:
            result = build_seq(lineId, seq)
            f.write(result + '\n')
            lineId = words[0]
            seq = []
            seq.append(words[1:3])

    f.write(build_seq(lineId, seq) + '\n')

