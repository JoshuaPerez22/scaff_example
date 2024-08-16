import sys
from abc import abstractmethod


class Car:
    def __init__(self, key="000", max_kmh=180):
        self.__key = key
        self.max_kmh = max_kmh
    key = "123"

    def get_key(self):
        return self.__key

    def show_information(self):
        print(f"Key: {self.__key} ; Maximum vel: {self.max_kmh}")

    @staticmethod
    def greeting():
        print("hola")


class Animal:
    def __init__(self, name, legs=4, wings=False):
        self.name = name
        self.legs = legs
        self.wings = wings

    @abstractmethod
    def speak(self):
        ...

    def can_fly(self):
        return self.wings


class Cat(Animal):
    def speak(self):
        print("miaw")

    def can_fly(self):
        return super().can_fly()


class Dog(Animal):
    def speak(self):
        print("goof")


def main():
    pass
    # cat: Animal = Cat("Alien")
    # cat.speak()
    # print(f'Can Alien fly? {cat.can_fly()}')
    #
    # dog: Animal = Dog("Kiara")
    # dog.speak()
    # print(f'Can Kiara fly? {dog.can_fly()}')

    # car = Car()
    # print(car.get_key())
    # car.greeting()
    #
    # car2 = Car(max_kmh=250)
    # car2.show_information()


if (__name__ == '__main__'):
    sys.exit(main())
