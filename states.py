from aiogram.fsm.state import State, StatesGroup


class ParsingForm(StatesGroup):
    city = State()
    position_key_words = State()
    languages = State()
    salary_from = State()
    salary_to = State()
    experience = State()
    employment = State()
    parsing_resource = State()


class Switchers(StatesGroup):
    run_script = State()


class ActionBlocker(StatesGroup):
    parsing_inbound = State()

