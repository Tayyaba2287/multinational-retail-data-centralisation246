import random
word_list = ['mango', 'stawberry','orange', 'apple', 'peach']
print(word_list)

word = random.choice(word_list)
print(word)

guess = input('Guess a single letter: ')
print('Your guess is:', guess)
if len(guess) == 1 and guess.isalpha():
    print('Good guess!')
else:
    print("Oops! That is not a valid input.")
