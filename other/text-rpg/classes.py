class Character(object):
    def __init__(self):
        pass

    def is_alive(self):
        return self.hp > 0 and True

    def stats(self):
        print("%s HP: %d" % (self.name, self.hp))
        print("%s Armor: %d" % (self.name, self.armor))
        print("%s Weapon: %s" % (self.name, self.weapon))
        print()

    def damage(self, value):
        self.hp -= value
        if self.hp < 0:
            self.hp = 0

    def heal(self, value):
        self.hp += value

    def attack(self, enemy=None):
        if enemy is None:
            return "Error! enemy is None"

        ### Roll
        tohit = random.randint(1, 20) + self.tohit
        dam = random.randint(weapons_dir[self.weapon]["Min_Damage"],weapons_dir[self.weapon]["Max_Damage"])
        print("%s attack %s:" % (self.name, enemy.name))

        if tohit > enemy.armor + 10:
            print("Roll is: %d. Hit!" % (tohit))
            print("Damage is: %d." % (dam))
            enemy.damage(dam)
        else:
            print("Roll is: %d. Miss!" % (tohit))
        print(enemy.stats())

class Hero(Character):
    def __init__(self, name=None, hp=None, tohit=0, armor=None, weight=None, weapon=None):
        self.name   = name
        self.hp     = hp
        self.tohit  = tohit
        self.armor  = armor
        self.weight = weight
        self.weapon = weapon

class Zombie(Character):
    def __init__(self, name=None, hp=None, tohit=0, armor=None, weight=None, weapon=None):
        self.name   = name
        self.hp     = hp
        self.tohit  = tohit
        self.armor  = armor
        self.weight = weight
        self.weapon = weapon

### LOCATIONS
class Location(object):
    def __init__(self, name, description):
        self.name = name
        self.description = description

class LocHomeBedroom(Location):
    def __init__(self,exit1,exit2,exit3)
        self.exit1 = exit1
        self.exit2 = exit2
        self.exit3 = exit3

class LocHomeBathroom(Location):
    def __init__(self,exit1,exit2,exit3)
        self.exit1 = exit1
        self.exit2 = exit2
        self.exit3 = exit3

class LocHomeHall(Location):
    def __init__(self,exit1,exit2,exit3)
        self.exit1 = exit1
        self.exit2 = exit2
        self.exit3 = exit3
