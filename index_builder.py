import json, pickle
import os, re, time, nltk
import multiprocessing as mp
from nltk.stem.wordnet import WordNetLemmatizer

def cleanPage(text_page):
    pat_link1 = re.compile("<a href[^>]*>")
    pat_link2 = re.compile("</a>")
    pat_letter = re.compile(r'[^a-zA-Z0-9 \']+')
    pat_is = re.compile("(it|he|she|that|this|there|here)(\'s)", re.I)
    pat_s = re.compile("(?<=[a-zA-Z])\'s")
    pat_s2 = re.compile("(?<=s)\'s?")
    pat_not = re.compile("(?<=[a-zA-Z])n\'t")
    pat_would = re.compile("(?<=[a-zA-Z])\'d")
    pat_will = re.compile("(?<=[a-zA-Z])\'ll")
    pat_am = re.compile("(?<=[I|i])\'m")
    pat_are = re.compile("(?<=[a-zA-Z])\'re")
    pat_ve = re.compile("(?<=[a-zA-Z])\'ve")

    new_text = pat_link1.sub("", text_page)
    new_text = pat_link2.sub("", new_text)
    new_text = pat_letter.sub(' ', new_text).strip().lower()
    new_text = pat_is.sub(r"\1 is", new_text)
    new_text = pat_s.sub("", new_text)
    new_text = pat_s2.sub("", new_text)
    new_text = pat_not.sub(" not", new_text)
    new_text = pat_would.sub(" would", new_text)
    new_text = pat_will.sub(" will", new_text)
    new_text = pat_am.sub(" am", new_text)
    new_text = pat_are.sub(" are", new_text)
    new_text = pat_ve.sub(" have", new_text)
    new_text = new_text.replace('\'', ' ')
    return new_text

def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return nltk.corpus.wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return nltk.corpus.wordnet.VERB
    elif treebank_tag.startswith('N'):
        return nltk.corpus.wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return nltk.corpus.wordnet.ADV
    else:
        return ''

def lemma(word):
    if word and word not in stopwords:
        tag = nltk.pos_tag([word])
        pos = get_wordnet_pos(tag[0][1])
        if pos:
            lemmatized_word = lmtzr.lemmatize(word, pos)
            return lemmatized_word
        else:
            return word

def wordLemma(words):
    new_words = pool.map(lemma, words)
    return new_words

def getWordsDict(text_page, docid, freq):
    words = cleanPage(text_page).split()
    lemmaWords = wordLemma(words)
    
    for lemma_word in lemmaWords:
        if not lemma_word:
            continue
        first_ch = lemma_word[0]
        if first_ch not in charlist[:-1]:
            first_ch = 'number'
        
        if lemma_word in words_dict[first_ch]:
            if docid in words_dict[first_ch][lemma_word]:
                words_dict[first_ch][lemma_word][docid][0] += 1
            else:
                words_dict[first_ch][lemma_word][docid] = [freq, len(lemmaWords)]
        else:
            words_dict[first_ch][lemma_word] = {}
            words_dict[first_ch][lemma_word][docid] = [freq, len(lemmaWords)]

def writeFile(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as f:
        pickle.dump(data, f)

def dictInit(list):
    words_dict = {}
    for char in list:
        words_dict[char] = {}
    return words_dict

if __name__ == '__main__':
    lmtzr = WordNetLemmatizer()
    with open('stoplist.txt', 'r') as f:
        stopwords = eval(f.read())

    charlist = [chr(ord('a') + i) for i in range(26)] + ['number']
    print("Index Charlist...", charlist)

    folder = './text/AA/'
    files = os.listdir(folder)
    pool = mp.Pool()

    # Initialize titleIdDict to store document titles and IDs
    titleIdDict = {}

    for file in files:
        filename = os.path.join(folder, file)
        print("Processing...", filename)
        start_time = time.time()
        input_file = open(filename, 'r')
        words_dict = dictInit(charlist)
        
        while True:
            line = input_file.readline()
            if not line:
                break
            line = json.loads(line)
            docid = line['id']
            title = line.get('title', f"Untitled {docid}")
            
            # Populate titleIdDict
            titleIdDict[docid] = title
            
            getWordsDict(line['text'], docid, 1)

        input_file.close()

        print("Counting...", sum([len(words_dict[i]) for i in charlist]))
        print("Time taken...", time.time() - start_time)
        print("Writing files...")

        for char in charlist:
            if words_dict[char]:
                writeFile(words_dict[char], f'./text/{char}/{file[-2:]}')

    # Save titleIdDict at the end
    writeFile(titleIdDict, './text/titleIdDict.pkl')
    print("titleIdDict has been saved successfully.")

