import random

class Npc(object):
    def __init__(self, name=None, hp=None, ac=None, attack=None):
        self.name = name
        self.hp = hp
        self.ac = ac
        self.attack = attack
        self.hit  = 0
        self.miss = 0

    def is_alive(self):
        return self.hp > 0 and True

    def stats(self):
        print("%s HP: %d" % (self.name, self.hp))
        print("%s HIT: %d" % (self.name, self.hit))
        print("%s MISS: %d" % (self.name, self.miss))

    def damage(self, value):
        self.hp -= value
        if self.hp < 0:
            self.hp = 0

    def kick(self, enemy=None):
        if enemy is None:
            return
        damage = random.randint(1, 20)

        print("%s attack %s:" % (self.name, enemy.name))
        if self.attack + damage >= enemy.ac + 10:
            dam = random.randint(1, 5)
            print("\tRoll: %d > %d - Hit!" % (
                    self.attack + damage, enemy.ac + 10))
            print("\tDamage: %d" % dam)
            print("\tHP: %d - %d = %d" % (
                    enemy.hp, dam, enemy.hp - dam))
            enemy.damage(dam)
            self.hit += 1
        else:
            print("\tRoll: %d < %d - Miss!" % (
                    self.attack + damage, enemy.ac + 10))
            self.miss += 1

turn = 0
npc1 = Npc(name='NPC1', hp=20, ac=10, attack=12)
npc2 = Npc(name='NPC2', hp=20, ac=12, attack=10)

while npc1.is_alive() and npc2.is_alive():
    turn += 1
    print("\nTurn: %d\n" % turn)

    npc1.kick(npc2)
    if not npc2.is_alive():
        break

    npc2.kick(npc1)
    if not npc1.is_alive():
        break

print("\n\nSummary:\n")
print("End turn: %d\n" % turn)
npc1.stats()
print
npc2.stats()
