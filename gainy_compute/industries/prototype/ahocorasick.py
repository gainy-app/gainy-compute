import re
import functools
from typing import Dict, List


def textclean_ahocorasick_createnode(stop_phrs_list):
    class Node:
        def __init__(self, word, is_final):
            self.word = word
            self.is_final = is_final
            self.subnodes = {}
            self.__go = {}
            self.__link = None
            self.parent = None
            self.length = 0
            self.__max_final_suffix = None
            self.substitution = None

        def subnode_exists(self, word):
            return word in self.subnodes

        def get_subnode(self, word):
            return self.subnodes[word]

        def add_subnode(self, node):
            self.subnodes[node.word] = node
            node.parent = self
            node.length = self.length + 1

        def print(self, prefix=""):
            for word, subnode in self.subnodes.items():
                print("%s%s%s:" % (prefix, word, "(F)" if subnode.is_final else ""))
                subnode.print(prefix + "  ")

        def get_link(self):
            if self.__link is None:
                if self.parent is None:
                    self.__link = self
                elif self.parent.parent is None:
                    self.__link = self.parent
                else:
                    self.__link = self.parent.get_link().go(self.word)
            return self.__link

        def go(self, word):
            if word not in self.__go:
                if word in self.subnodes:
                    self.__go[word] = self.subnodes[word]
                elif self.parent is None:
                    self.__go[word] = self
                else:
                    self.__go[word] = self.get_link().go(word)
            return self.__go[word]

        def get_max_final_suffix(self):
            if self.__max_final_suffix is None:
                if self.is_final or self.parent is None:
                    self.__max_final_suffix = self.length, self.substitution
                else:
                    self.__max_final_suffix = self.get_link().get_max_final_suffix()
            return self.__max_final_suffix

    def split_words(line):
        return list(filter(lambda l: len(l) > 0, re.split(r'([\W])', line)))

    def add_line(root_node, line, substitution):
        words = split_words(line)
        node = root_node
        for word in words:
            if node.subnode_exists(word):
                node = node.get_subnode(word)
            else:
                new_node = Node(word, False)
                node.add_subnode(new_node)
                node = new_node
        node.is_final = True
        node.substitution = substitution

    # charge! (load the AhiCorasick root_node)
    root_node = Node("", False)
    for stop_phrase, substitution in stop_phrs_list.items():
        if (len(stop_phrase) < 1): continue
        add_line(root_node, stop_phrase, substitution)
    return root_node


def textclean_ahocorasick_processtext(input_text, root_node):
    def split_words(line):
        return list(filter(lambda l: len(l) > 0, re.split(r'([\W])', line)))

    def remove_stop_words(line, root_node):
        words = split_words(line.rstrip())
        start_pos = 0
        blacklisted_intervals = []
        node = root_node
        for (k, word) in enumerate(words):
            node = node.go(word)
            max_final_suffix_len, substitution = node.get_max_final_suffix()
            if max_final_suffix_len > 0:
                blacklisted_intervals.append((k - max_final_suffix_len + 1, k, substitution))

        def compare(interval1, interval2):
            if interval1[0] != interval2[0]:
                return interval1[0] - interval2[0]
            return interval2[1] - interval1[1]

        blacklisted_intervals.sort(key=functools.cmp_to_key(compare))
        result = []
        l = k = 0
        while k < len(words):
            while l < len(blacklisted_intervals) and blacklisted_intervals[l][1] < k:
                l += 1
            if l >= len(blacklisted_intervals) or blacklisted_intervals[l][0] > k:
                result += words[k]
                k += 1
            elif blacklisted_intervals[l][0] == k:
                result += blacklisted_intervals[l][2]
                k = blacklisted_intervals[l][1] + 1
            else:
                k += 1
        return re.sub(r"\s+", " ", "".join(result)).strip()

    # fire!
    if len(input_text) < 1:
        return input_text

    return remove_stop_words(input_text, root_node)
