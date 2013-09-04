#!/usr/bin/python3.1

import random
from classes import *



### Directories

weapons_dir = {
   "Fists" : {'Description' : 'Bare hands',
             'Weight' : 0,
             'Min_Damage' : 1,
             'Max_Damage' : 2,
            },
   "Sword" : {'Description' : 'Sharp, cold, deadly',
             'Weight' : 1,
             'Min_Damage' : 2,
             'Max_Damage' : 6,
            }
    }

### Create Hero
print("Create your Hero\n")
#start_name = input("Type your name: ")

hero = Hero(name = 'Hero', hp=10, armor=0, weapon="Sword")
zombie = Zombie(name="Zombie", hp=5, armor=0,  weapon="Fists")


######## House
### Text 
intro_text1="Ты проснулся от громкого шума на улице. Шум от крика сотен людей. Это было похоже на очередной разгон демонстрации протестующих, но в этот раз что-то было иначе. В этих криках был слышен животный ужас. Что-то явно пошло не так."


print(intro_text1)
turn = 0
"""
while hero.is_alive() and zombie.is_alive():
    hero.attack(zombie)
    if not zombie.is_alive():
        break
    zombie.attack(hero)
    if not hero.is_alive():
        break
"""
