import enum
import logging
import asyncio
from aiogram import types, Dispatcher, F, Router
from aiogram.types import FSInputFile
from datetime import datetime
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
import os
from states import *
from database_manager import *
from parsers import *

router = Router()


class KeyBoards(enum.Enum):
    MAIN_MENU = 0
    CITIES_KEYBOARD = 1
    EMPLOYMENT = 2
    BACK_TO_MAIN = 3
    LANGUAGES_KEYBOARD = 4
    SALARY = 5
    EXPERIENCE_KEYBOARD = 6
    RESOURCE_KEYBOARD = 7
    PRE_RUN_KEYBOARD = 8


def get_keyboards(keyboard_id):
    def main_menu():
        buttons = [KeyboardButton(text=t) for t in ["Parse Candidates", "Parsing History"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def cities():
        buttons = [KeyboardButton(text=t) for t in ["Dnipro", "Kyiv", "Odesa", "All Cities", "Remote", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def employment():
        buttons = [KeyboardButton(text=t) for t in ["Full time", "Part time", "Both", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def back_to_main():
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Return to main menu")]])

    def languages():
        buttons = [KeyboardButton(text=t) for t in ["German", "Ukrainian", "russian", "English", "French", "Slovakian", "Poland", "Continue", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def salary():
        buttons = [KeyboardButton(text=t) for t in ["Not specified", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def experience():
        buttons = [KeyboardButton(text=t) for t in ["No experience", "Less then 1 year", "1-3 years", "3-5 years", "5+ years", "Continue", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def resource():
        buttons = [KeyboardButton(text=t) for t in ["WorkUA", "RabotaUA", "Both", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    def pre_run():
        buttons = [KeyboardButton(text=t) for t in ["Run Script", "Return to main menu"]]
        return ReplyKeyboardMarkup(keyboard=[[button] for button in buttons], resize_keyboard=True)

    if keyboard_id == KeyBoards.MAIN_MENU:
        return main_menu()
    elif keyboard_id == KeyBoards.CITIES_KEYBOARD:
        return cities()
    elif keyboard_id == KeyBoards.EMPLOYMENT:
        return employment()
    elif keyboard_id == KeyBoards.BACK_TO_MAIN:
        return back_to_main()
    elif keyboard_id == KeyBoards.LANGUAGES_KEYBOARD:
        return languages()
    elif keyboard_id == KeyBoards.SALARY:
        return salary()
    elif keyboard_id == KeyBoards.EXPERIENCE_KEYBOARD:
        return experience()
    elif keyboard_id == KeyBoards.RESOURCE_KEYBOARD:
        return resource()
    elif keyboard_id == KeyBoards.PRE_RUN_KEYBOARD:
        return pre_run()


def create_dates_inline_keyboard(dates):
    buttons = [
        [InlineKeyboardButton(text=date, callback_data=f"date_{date}") for date in dates]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(F.text == "Parsing History")
async def check_history(msg: types.Message, state: FSMContext):
    await msg.answer(text="Choose date to fetch history")

    with DataBaseManager() as db:
        dates = db.fetch_all_ids()

    inline_kb = create_dates_inline_keyboard(dates)
    await msg.answer("Select a date to view parsing history:", reply_markup=inline_kb)


@router.callback_query(F.data.startswith("date_"))
async def fetch_and_send_history(callback_query: types.CallbackQuery, state: FSMContext):
    date_key = callback_query.data.split("_")[1]
    user_id = callback_query.from_user.id

    with DataBaseManager() as db:
        document = db.fetch_data(date_key)

    if not document:
        await callback_query.message.answer(f"No data found for {date_key}")
        return

    filenames = save_parsing_history_to_excel(date_key)
    for filename in filenames:
        file = FSInputFile(filename)
        await callback_query.message.answer_document(file)
        os.remove(filename)
    await callback_query.answer()


@router.message(ActionBlocker.parsing_inbound)
async def block_action(msg: types.Message, state: FSMContext):
    await msg.answer(text="Wait until parsing will end")


@router.message(Command("start"))
async def cmd_start(msg: types.Message):
    await msg.answer(
                text="This bot written by https://github.com/Bersenrar as test task\nMany moments in this application"
                     "can be implemented better but in order to complete TEST task faster and just show my abbility"
                     "to create such application they weren't implemented as good as possible because it would take"
                     "long time(+-5 days)",
                reply_markup=get_keyboards(KeyBoards.MAIN_MENU)
            )


@router.message(F.text == "Return to main menu")
async def return_to_main(msg: types.Message, state: FSMContext):
    await msg.answer(text="Back to main menu", reply_markup=get_keyboards(KeyBoards.MAIN_MENU))
    await state.clear()


@router.message(F.text == "Parse Candidates")
async def start_form_filling(msg: types.Message, state: FSMContext):
    await msg.answer(text="To parse candidate fill parameters")
    await msg.answer(
            text="Write key words for searching through whitespace for example: Python Developer",
            reply_markup=get_keyboards(KeyBoards.BACK_TO_MAIN)
    )
    await state.set_state(ParsingForm.position_key_words)


@router.message(ParsingForm.position_key_words)
async def parse_key_words(msg: types.Message, state: FSMContext):
    await state.update_data(position=msg.text.strip())
    await msg.answer(
        text="Choose city using keyboard or write by your own in English",
        reply_markup=get_keyboards(KeyBoards.CITIES_KEYBOARD)
    )
    await state.set_state(ParsingForm.city)


@router.message(ParsingForm.city)
async def get_city(msg: types.Message, state: FSMContext):
    await state.update_data(city=msg.text.strip().lower())
    await msg.answer(
            text="Now choose employment type",
            reply_markup=get_keyboards(KeyBoards.EMPLOYMENT)
    )
    await state.set_state(ParsingForm.employment)


@router.message(ParsingForm.employment)
async def get_employment(msg: types.Message, state: FSMContext):
    await state.update_data(employment=msg.text.strip())
    await state.set_state(ParsingForm.languages)
    await msg.answer(text="Select must have languages to skip this parameter press continue button", reply_markup=get_keyboards(KeyBoards.LANGUAGES_KEYBOARD))


@router.message(ParsingForm.languages)
async def language_selection(msg: types.Message, state: FSMContext):
    user_data = await state.get_data()
    selected_languages = user_data.get("selected_languages", [])

    print(f"Current state: {await state.get_state()}")

    if msg.text == "Continue":
        await msg.answer("Languages selection completed.")
        await state.update_data(selected_languages=selected_languages)
        await msg.answer(text="Now input salary from search", reply_markup=get_keyboards(KeyBoards.SALARY))
        await state.set_state(ParsingForm.salary_from)
    elif msg.text in ["German", "Ukrainian", "russian", "English", "French", "Slovakian", "Poland"]:
        if msg.text not in selected_languages:
            selected_languages.append(msg.text)
            await state.update_data(selected_languages=selected_languages)
            await msg.answer(f"Added {msg.text}. You can select more or press 'Continue'.")
        else:
            await msg.answer(f"{msg.text} is already selected. You can select more or press 'Continue'.")
    else:
        await msg.answer("Please choose a valid language or press 'Continue'.")

    if not msg.text == "Continue":
        await msg.answer(
            text="Select must-have languages or press 'Continue' when done:"
        )


@router.message(ParsingForm.salary_from)
async def get_salary_to(msg: types.Message, state: FSMContext):
    await state.update_data(salary_from=msg.text)
    await msg.answer(text="Now specify salary to", reply_markup=get_keyboards(KeyBoards.SALARY))
    await state.set_state(ParsingForm.salary_to)


@router.message(ParsingForm.salary_to)
async def get_salary_from(msg: types.Message, state: FSMContext):
    await state.update_data(salary_from=msg.text)
    await msg.answer(text="Select experience to skip this parameter press continue button", reply_markup=get_keyboards(KeyBoards.EXPERIENCE_KEYBOARD))
    await state.set_state(ParsingForm.experience)


@router.message(ParsingForm.experience)
async def experience_selection(msg: types.Message, state: FSMContext):
    user_data = await state.get_data()
    selected_experience = user_data.get("selected_experience", [])

    if msg.text == "Continue":
        await msg.answer("Experience selection completed.")
        await state.update_data(selected_experience=selected_experience)
        await msg.answer(text="Now you need select resource for parsing.", reply_markup=get_keyboards(KeyBoards.RESOURCE_KEYBOARD))
        await state.set_state(ParsingForm.parsing_resource)
    elif msg.text in ["No experience", "Less then 1 year", "1-3 years", "3-5 years", "5+ years", "Not specified"]:
        if msg.text not in selected_experience:
            selected_experience.append(msg.text)
            await state.update_data(selected_experience=selected_experience)
            await msg.answer(f"Added {msg.text}. You can select more or press 'Continue'.")
        else:
            await msg.answer(f"{msg.text} is already selected. You can select more or press 'Continue'.")
    else:
        await msg.answer("Please choose a valid experience option or press 'Continue'.")

    if not msg.text == "Continue":
        await msg.answer(
            text="Select experience or press 'Continue' when done:"
        )


@router.message(ParsingForm.parsing_resource)
async def get_resource(msg: types.Message, state: FSMContext):
    await state.update_data(resource=msg.text)
    await msg.answer(text="All queries have been filled", reply_markup=get_keyboards(KeyBoards.PRE_RUN_KEYBOARD))
    await state.set_state(Switchers.run_script)


@router.message(F.text == "Run Script")
@router.message(Switchers.run_script)
async def parse_data(msg: types.Message, state: FSMContext):
    def prepare_data(d):
        langs_table = {
            "English": "eng", "Ukrainian": "ua", "russian": "ru", "Poland": "pol",
            "German": "ger", "Slovakian": "slav", "French": "fre"
        }

        experience_table = {
            "No experience": "no_experience", "Less then 1 year": "less_1_year",
            "1-3 years": "1_3_years", "3-5 years": "3_5_years", "5+ years": "5_more_years"
        }

        employment_table = {"Full time": "full_time", "Part time": "part_time", "Both": "both"}

        def prepare_work_ua():
            query = {
                "position": d.get("position", "").split(" "),
                "city": d.get("city", "").lower(),
                "employment": [employment_table.get(d.get("employment", ""), "both")],
                "salary_from": d.get("salary_from") if d.get("salary_from", "").isdigit() else None,
                "salary_to": d.get("salary_to") if d.get("salary_to", "").isdigit() else None,
                "language": [langs_table.get(lang) for lang in d.get("selected_languages", [])],
                "experience": [experience_table.get(exp) for exp in d.get("selected_experience", [])],
            }
            query = {k: v for k, v in query.items() if v}
            return query

        def prepare_rabota_ua():
            query = {
                "position": d.get("position", "").split(" "),
                "city": d.get("city", "").capitalize(),
                "employment": employment_table.get(d.get("employment", ""), "both"),
                "salary_from": d.get("salary_from") if d.get("salary_from", "").isdigit() else None,
                "salary_to": d.get("salary_to") if d.get("salary_to", "").isdigit() else None,
                "language": [langs_table.get(lang) for lang in d.get("selected_languages", [])],
                "experience": [experience_table.get(exp) for exp in d.get("selected_experience", [])],
            }
            query = {k: v for k, v in query.items() if v}
            return query

        resource = d.get("resource")
        print(resource)
        if resource == "Both":
            return prepare_work_ua(), prepare_rabota_ua()
        elif resource == "WorkUA":
            return prepare_work_ua(), None
        else:
            return None, prepare_rabota_ua()

    def parse_work_ua(q):
        parser = WorkuaParser()
        result = parser.run_script(q)
        with MarksManager() as m:
            for d in result:
                d["mark"] = m.count_mark_workua(d)
        result.sort(key=lambda x: x["mark"], reverse=True)
        with DataBaseManager() as db:
            db.append_data(
                date_key=datetime.now().strftime("%d.%m.%Y"),
                query=" ".join(q.get("position", "ALL")),
                resource_name="WORK_UA",
                data=result
            )

    async def parse_rabota_ua(q):
        parser = RabotaUa()
        result = await asyncio.to_thread(parser.run_script, q)

        with MarksManager() as m:
            for d in result:
                d["mark"] = m.count_mark_rabotaua(d)
        result.sort(key=lambda x: x["mark"], reverse=True)
        with DataBaseManager() as db:
            db.append_data(
                date_key=datetime.now().strftime("%d.%m.%Y"),
                query=" ".join(q.get("position", "ALL")),
                resource_name="RABOTA_UA",
                data=result
            )

    user_query = await state.get_data()
    resource = user_query.get("resource")
    await state.clear()
    await state.set_state(ActionBlocker.parsing_inbound)
    work_ua_query, rabota_ua_query = prepare_data(user_query)
    print(work_ua_query, rabota_ua_query)
    await msg.answer(text="Wait till parsing ends", reply_markup=ReplyKeyboardRemove())

    if resource == "WorkUA":
        parse_work_ua(work_ua_query)
    elif resource == "RabotaUA":
        await parse_rabota_ua(rabota_ua_query)
    elif resource == "Both":
        parse_work_ua(work_ua_query)
        await parse_rabota_ua(rabota_ua_query)

    await msg.answer(text="Parsing ended", reply_markup=get_keyboards(KeyBoards.MAIN_MENU))

