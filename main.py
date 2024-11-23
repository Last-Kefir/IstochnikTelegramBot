import asyncio
import re

import msgspec
import mysql.connector
import requests
import json

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from loguru import logger
from datetime import datetime
from aiogram.utils.exceptions import (MessageNotModified, MessageCantBeDeleted, MessageToDeleteNotFound,
                                      MessageToEditNotFound)

from apscheduler.schedulers.asyncio import AsyncIOScheduler 

from config.config import token, host, user, password, db_name, url, log_file, dict_to_doctor

try:
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    mycursor = mydb.cursor(buffered=True)

except mysql.connector.Error as err:
    logger.error("Connection to DB problem: ", err)
except Exception as err:
    logger.error(err)

user_choices = {}
prices = {}
# Включаем логирование, чтобы не пропустить важные сообщения
logger.add(log_file, format="{time} {level} {message}", level="INFO", rotation="1 day", compression="zip",
           encoding="utf-8",
           backtrace=True,
           diagnose=True)

# Объект бота
bot = Bot(token)
# Диспетчер
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


class UsersData(StatesGroup):
    fio = State()
    birth_date = State()
    phone_number = State()
    phone_number_validation = State()


# You can use state '*' if you need to handle all states
# @dp.message_handler(state='*', commands='<-- Назад')
# @dp.callback_query_handler(Text(equals='cancel_input', ignore_case=True), state='*')
# async def cancel_handler(callback_query: types.CallbackQuery, state: FSMContext):
#     # # # # logger.debug('DEBUG: Зашли в cancel_input handler')
#     """
#     Allow user to cancel any action
#     """
#     current_state = await state.get_state()
#     # # # # logger.debug('DEBUG: Пришли из state = ', current_state, type(current_state))
#     if current_state is None:
#         return
#     logger.error('Cancelling state %r', current_state)
#     # Cancel state and inform user about it
#     await state.finish()
#     final_state = await state.get_state()
#     # # # logger.debug('DEBUG: state after finish =', final_state)
#     # And remove keyboard (just in case)
#     await edit_message_with_choices(callback_query)
#     await bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
#     # # # logger.debug('DEBUG: Удаляем сообщение')
#     # await process_patient_data(callback_query)
#     await callback_query.answer('doctime_')


@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    try:
        mid = await check_mid(message.chat.id)
        if mid:
            try:
                await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
                await bot.edit_message_text(chat_id=message.chat.id, message_id=mid[1], text='Отменено')
                await insert_mid(message.chat.id, None)
            except Exception as e:
                logger.error(
                    f"Вылезла какая-то ошибка от телеги на редактирование сообщения в чате {message.chat.id} {message.chat.first_name}: '{e}'. Но мы это проигнорим.")
        logger.info(f'{message.chat.id} {message.chat.first_name} нажал /start')

        mess = f'Здравствуйте, <b>{message.chat.first_name}</b>!\nВы обратились к боту клиники Источник, ' \
               'могу записать вас к врачу.'
        kb = [
            [
                types.KeyboardButton(text=f'Записаться к врачу')
            ],
        ]
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=kb,
            resize_keyboard=True,
            input_field_placeholder="Меню",
            one_time_keyboard=True
        )
        await bot.send_message(message.chat.id, mess, reply_markup=keyboard, parse_mode='html')
    except Exception as err:
        logger.error(err)


async def split(data):
    try:
        data = data.rpartition('_')[2]
        return data
    except Exception as err:
        logger.error(err)


@dp.callback_query_handler(lambda c: c.data, state='*')
async def process_slots_by_specs(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        # В первую очередь инициализируем словарь выбора для этого chatID, если он еще не инициализирован
        if state:
            await state.finish()
        if user_choices.get(callback_query.message.chat.id) is None:
            user_choices[callback_query.message.chat.id] = {}
        if callback_query.data.startswith('branch_') or callback_query.data.startswith('back_docprice_'):
            # Сюда мы попадем, если выбрали филиал или если вернулись из выбора даты
            # В обоих случаях надо запросить специальности, указав только что выбранный или выбранный ранее филиал
            await edit_message_with_choices(callback_query)
            selected_brunch = await split(callback_query.data)
            if callback_query.data.startswith('back_docprice_'):
                pass
            elif selected_brunch == "any":
                user_choices[callback_query.message.chat.id]['qqc244branch'] = ""
                user_choices[callback_query.message.chat.id]['qqc244branchname'] = "любой"
            else:
                user_choices[callback_query.message.chat.id]['qqc244branch'] = await split(callback_query.data)
                user_choices[callback_query.message.chat.id]['qqc244branchname'] = await get_pressed_button_text(
                    callback_query)
            logger.info(
                f'{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал филиал: {user_choices[callback_query.message.chat.id]["qqc244branchname"]} {user_choices[callback_query.message.chat.id]["qqc244branch"]}')
            await get_keyboard(callback_query.message,
                               await get_specs(callback_query.message,
                                               user_choices[callback_query.message.chat.id]['qqc244branch']),
                               'Выберите специализацию', 'spec_')
        elif callback_query.data.startswith('spec_') or callback_query.data.endswith(
                '_datetime_') or callback_query.data.endswith('_datetime_bydoc_'):
            # Сюда попадаем, если выбрали специальность врача или если вернулись из выбора конкретного доктора и его времени
            # Надо показать доступные даты для выбранной специальности врача и выбранного филиала
            try:
                await edit_message_with_choices(callback_query)
                if callback_query.data.startswith('spec_'):
                    spec = await get_pressed_button_text(callback_query)
                else:
                    spec = user_choices[callback_query.message.chat.id]['spec']
                if spec != 'ЛЮБОЙ':
                    user_choices[callback_query.message.chat.id]['spec'] = spec
                    logger.info(
                        f'{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал '
                        f'специализацию: {user_choices[callback_query.message.chat.id]["spec"]}')
                slots = await get_slots(callback_query.message,
                                        user_choices[callback_query.message.chat.id]['spec'],
                                        user_choices[callback_query.message.chat.id]['qqc244branch'],
                                        qqc244='false'
                                        )
                slots = slots['slots']
                await get_keyboard(callback_query.message, slots,
                                   'Выберите врача если знаете к кому хотите записаться, либо выберите пункт меню '
                                   '"ЛЮБОЙ"',
                                   'docprice_')
            except Exception as err:
                logger.error(err)
        elif callback_query.data.startswith('docprice_') or callback_query.data.startswith('back_doctime_'):
            try:
                await edit_message_with_choices(callback_query)
                if callback_query.data == 'docprice_any' or (
                        callback_query.data.startswith("back_") and user_choices[callback_query.message.chat.id].get(
                    "anyDoctor") == 'True'):
                    await add_to_form(callback_query.message.chat.id, "anyDoctor", 'True')
                    logger.info(
                        f'{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал пункт меню '
                        f'"ЛЮБОЙ" (врач).')
                    user_choices[callback_query.message.chat.id]['doc_qqc'] = ''
                    slots = await get_slots(callback_query.message,
                                            user_choices[callback_query.message.chat.id]['spec'],
                                            user_choices[callback_query.message.chat.id]['qqc244branch'],
                                            qqc244='')

                else:
                    await add_to_form(callback_query.message.chat.id, "anyDoctor", 'False')
                    if callback_query.data.startswith('docprice_'):
                        doc_qqc = await split(callback_query.data)
                        logger.info(
                            f'{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал '
                            f'записаться к определенному врачу: {await get_pressed_button_text(callback_query)} ({doc_qqc})')
                        user_choices[callback_query.message.chat.id]['doc_qqc'] = doc_qqc
                    slots = await get_slots(callback_query.message,
                                            user_choices[callback_query.message.chat.id]['spec'],
                                            user_choices[callback_query.message.chat.id]['qqc244branch'], "",
                                            user_choices[callback_query.message.chat.id]['doc_qqc'])
                slots = slots['slots']
                slots = slots[0]
                dates = {}
                for x in slots:
                    dates[slots[x].get('day')] = slots[x].get('data')
                if dates:
                    if 'doc_qqc' in user_choices[callback_query.message.chat.id]:
                        if user_choices[callback_query.message.chat.id]['doc_qqc'] != "":
                            await get_keyboard(callback_query.message, dates, 'Выберите дату', 'datetime_bydoc_')
                        else:
                            await get_keyboard(callback_query.message, dates, 'Выберите дату', 'datetime_')
                else:
                    await get_keyboard(callback_query.message, dates,
                                       'К сожалению нет доступных дат в ближайшее время',
                                       'datetime_')
            except Exception as err:
                logger.error(err)

        elif callback_query.data.startswith('datetime_') or callback_query.data.endswith('_savedusers_'):
            # Сюда попадаем, если выбрали дату, на которую хоти записаться, или если вернулись с выбора времени
            # Выводим список врачей и их времен, получив их на основе филиала, специальности и даты
            await edit_message_with_choices(callback_query)
            if callback_query.data.startswith('datetime_'):
                user_choices[callback_query.message.chat.id]['day'] = await split(callback_query.data)
            logger.info(
                f'{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал дату: {user_choices[callback_query.message.chat.id]["day"]}')
            pressed_button_text = await get_pressed_button_text(callback_query)
            if pressed_button_text not in ['Отмена', '<-- Назад']:
                user_choices[callback_query.message.chat.id]['dayName'] = pressed_button_text
            # elif await get_pressed_button_text(callback_query):
            #     user_choices[callback_query.message.chat.id]['dayName'] = await get_pressed_button_text(
            #         callback_query)
            # Вот тут получим слоты на базе специальности, филиала и даты
            if callback_query.data.startswith('datetime_bydoc_') or user_choices[callback_query.message.chat.id].get(
                    'anyDoctor') == "False":
                slots = await get_slots(callback_query.message,
                                        user_choices[callback_query.message.chat.id]['spec'],
                                        user_choices[callback_query.message.chat.id]['qqc244branch'],
                                        user_choices[callback_query.message.chat.id]['day'],
                                        user_choices[callback_query.message.chat.id]['doc_qqc'])
            else:
                slots = await get_slots(callback_query.message,
                                        user_choices[callback_query.message.chat.id]['spec'],
                                        user_choices[callback_query.message.chat.id]['qqc244branch'],
                                        user_choices[callback_query.message.chat.id]['day'])
            slots = slots['slots']
            for x in slots:
                schedule = {}
                text_to_deliver = "Записаться к врачу: " + (
                        x.get('fio') + "\n" + 'Стоимость приема: ' + str(
                    x.get('price')) + ' руб.\n' + 'Доступное время:')
                for y in x['schedule']:
                    schedule[y['time2appoint']] = y['time']
                await get_keyboard(callback_query.message, schedule, text_to_deliver,
                                   'doctime_' + x.get('qqc') + '_')
                prices[x.get('qqc')] = x.get('price')
        elif callback_query.data.startswith('doctime_') or callback_query.data.startswith('backdoctime_'):
            # Сюда мы попали, выбрав конкретное время у конкретного доктора.
            if callback_query.data.startswith('doctime_'):
                user_choices[callback_query.message.chat.id]['doc_qqc'] = await split(
                    callback_query.data.rpartition('_')[0])
                doc_info = await get_doc_det(callback_query.message,
                                             user_choices[callback_query.message.chat.id]['doc_qqc'])
                user_choices[callback_query.message.chat.id]['doc_fio'] = doc_info["docName"]
                user_choices[callback_query.message.chat.id]['doc_filial'] = doc_info["filialName"]
                user_choices[callback_query.message.chat.id]['time'] = await split(callback_query.data)
                user_choices[callback_query.message.chat.id]['timeShort'] = await get_pressed_button_text(
                    callback_query)
            else:
                # Мы пришли сюда по "backdoctime_", надо бы убрать 'qqc_pat', чтобы "забыть" ранее выбранного пациента
                # Да и номер телефона тоже бы почистить?
                user_choices[callback_query.message.chat.id].pop('qqc_pat', None)

            await edit_message_with_choices(callback_query,
                                            f"Нажали: {user_choices[callback_query.message.chat.id]['doc_fio']}, {user_choices[callback_query.message.chat.id]['timeShort']}")

            logger.info(
                f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал врача и время: {user_choices[callback_query.message.chat.id]['doc_fio']} ({user_choices[callback_query.message.chat.id]['doc_qqc']}) {user_choices[callback_query.message.chat.id]['time']}")

            await process_patient_data(callback_query)

        elif callback_query.data.startswith('savedusers_'):
            await edit_message_with_choices(callback_query)
            cid = callback_query.message.chat.id
            clbk = await split(callback_query.data)
            button_text = await get_pressed_button_text(callback_query)
            if clbk == 'Другого':
                # try:
                #     await bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
                # except aiogram_exceptions.MessageCantBeDeleted:
                #     pass
                logger.info(
                    f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал записать ранее неизвестного пациента.")

                await get_users_info(callback_query.message)
            else:
                try:
                    sql = "SELECT chatID,fio,phone,birthdate FROM users_info WHERE chatID = %s and fio = %s;"
                    chat_id = (cid, button_text)
                    mycursor.execute(sql, chat_id)
                    myresult = mycursor.fetchall()
                    myresult = myresult[0]

                    user_choices[cid]['fio'] = str(myresult[1])
                    user_choices[cid]['phone'] = str(myresult[2])
                    user_choices[cid]['birthdate'] = str(myresult[3])

                    logger.info(
                        f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} выбрал записать пациента: {user_choices[callback_query.message.chat.id]['fio']} {user_choices[callback_query.message.chat.id]['birthdate']}")

                    # await bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
                    await reasuring(callback_query.message)
                except mysql.connector.OperationalError as e:
                    logger.error('error in connection to database: '.format(e))
                    logger.warning("try to reconnect to db")
                    mydb.reconnect(3, 2)
                except mysql.connector.Error as err:
                    logger.error('error in check_mid, ', err)
                except Exception as err:
                    logger.error(err)
        elif callback_query.data == 'yes':
            logger.info(
                f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} пытается записать {user_choices[callback_query.message.chat.id]['fio']} ({user_choices[callback_query.message.chat.id]['birthdate']}) к специалисту {user_choices[callback_query.message.chat.id]['doc_fio']} ({user_choices[callback_query.message.chat.id]['doc_qqc']}) ({user_choices[callback_query.message.chat.id]['spec']}) на {user_choices[callback_query.message.chat.id]['day']} {user_choices[callback_query.message.chat.id]['time']}")

            mid = await check_mid(callback_query.message.chat.id)
            mid = mid[1]

            await bot.edit_message_reply_markup(callback_query.message.chat.id, callback_query.message.message_id,
                                                reply_markup=None)
            try:
                sql = "delete from users where chatID=%s and msgID=%s"
                val = (callback_query.message.chat.id, mid)
                mycursor.execute(sql, val)
                mydb.commit()
            except mysql.connector.OperationalError as e:
                logger.error('error in connection to database: '.format(e))
                logger.warning("try to reconnect to db")
                mydb.reconnect(3, 2)
            except mysql.connector.Error as err:
                logger.error(err)
            except Exception as err:
                logger.error(err)
            await post_appointment(callback_query.message)
        elif callback_query.data == 'no':
            logger.info(
                f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} нажал отмена записи.")

            mid = await check_mid(callback_query.message.chat.id)
            mid = mid[1]

            await bot.edit_message_reply_markup(chat_id=callback_query.message.chat.id,
                                                message_id=callback_query.message.message_id,
                                                reply_markup=None)
            await bot.edit_message_text(chat_id=callback_query.message.chat.id,
                                        message_id=callback_query.message.message_id,
                                        text='Отменено')
            await bot.send_message(callback_query.message.chat.id, 'Может быть позже')
            await cmd_start(callback_query.message)
        elif callback_query.data == 'cancel':
            logger.info(
                f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} нажал отмена при заполнении данных пациента или при выборе филиала.")
            mid = await check_mid(callback_query.message.chat.id)
            await bot.edit_message_reply_markup(callback_query.message.chat.id, mid[1], reply_markup=None)
            await bot.edit_message_text(chat_id=callback_query.message.chat.id, message_id=mid[1], text='Отменено')
            await insert_mid(callback_query.message.chat.id, None)
            user_choices.pop(callback_query.message.chat.id, None)
            await cmd_start(callback_query.message)
        elif callback_query.data.startswith('back_spec_'):
            await edit_message_with_choices(callback_query)
            branches = await get_branches(callback_query.message)
            if branches:
                await get_keyboard(callback_query.message, branches, 'Выберите филиал', 'branch_')
            else:
                logger.info(
                    f"{callback_query.message.chat.id} {callback_query.message.chat.first_name} нажал отмена при заполнении данных пациента или при выборе филиала.")
                await edit_messages()
                user_choices.pop(callback_query.message.chat.id, None)
                await cmd_start(callback_query.message)
        elif callback_query.data.startswith('Записаться'):
            await edit_message_with_choices(callback_query)
            await make_appointment(callback_query.message)
    except KeyError as err:
        logger.error(f'Ошбика KeyError {err}')
        await bot.send_message(callback_query.message.chat.id,
                               text=f'<b>К сожалению что-то пошло не так, попробуйте ещё раз.</b>', parse_mode='html')
        await make_appointment(callback_query.message)
    except Exception as err:
        logger.error(err)
        await bot.send_message(callback_query.message.chat.id,
                               text=f'<b>К сожалению что-то пошло не так, попробуйте ещё раз.</b>', parse_mode='html')
        await make_appointment(callback_query.message)


@dp.errors_handler(exception=MessageToDeleteNotFound)
async def message_cant_be_deleted(update, error, message: types.Message):
    await insert_mid(message.chat.id, None)
    await cmd_start(message)


async def process_patient_data(clbk):
    try:
        user_data = await user_data_pulling(clbk.message.chat.id)
        if user_data:
            patients = []
            for patient in user_data:
                patients.append(str(patient[2]))
            await get_keyboard(clbk.message, patients, 'Кого хотите записать?', 'savedusers_')
        else:
            await get_users_info(clbk.message)
    except Exception as err:
        logger.error(err)


async def add_to_form(chat_id, key, value):
    try:
        if user_choices.get(chat_id) is None:
            user_choices[chat_id] = {}
        user_choices[chat_id][key] = value
    except Exception as err:
        logger.error(err)


async def format_numbers(phone_number: str) -> str:
    numbers = list(filter(str.isdigit, phone_number))[1:]
    return "8{}{}{}{}{}{}{}{}{}{}".format(*numbers)


async def decode_data(message, url, form):
    # r = ('{"result":"success","branches":{"":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f\u043f'
    #      '\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 ('
    #      '\u0443\u043b.40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
    #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC","orgqqc":"\u0422ACdAAFAqAA"},'
    #      '"\u0422ACdAAn"}')
    # r = bytes(r.encode('utf-8'))
    r = requests.post(url, form)
    r = bytes(r.text.encode('utf-8'))
    try:
        decoded_data = msgspec.json.decode(r)
        return decoded_data
    except msgspec.MsgspecError as e:
        logger.error(f"Ошибка при разборе JSON: {e} - {r}")
        await bot.send_message(message.chat.id,
                               text=f'<b>К сожалению не удалось загрузить список филиалов, попробуйте ещё раз.</b>',
                               parse_mode='html')
        await cmd_start(message)


async def get_branches(message: types.Message):
    try:
        get_branches_data = {'chatid': message.chat.id
                             # 'action': 'branch_list'
                             }
        await bot.send_chat_action(chat_id=message.chat.id, action='typing')
        # r = requests.post(f'{url}branch_list/', get_branches_data)
        # r = bytes(r.text.encode('utf-8'))
        # branches = msgspec.json.decode(r)
        # r = ('{"result":"success","branches":{"\u0000":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f '
        #      '\u043f\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 (\u0443\u043b. '
        #      '40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
        #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC",'
        #      '"orgqqc":"\u0422ACdAAFAqAA"},"\u0422ACdAAn"}')
        link = f'{url}branch_list/'
        decoded_data = await decode_data(message, link, get_branches_data)
        branches = decoded_data['branches']
        branches_obj = {}
        for x in branches:
            branches_obj[branches[x].get('title')] = branches[x].get('qqc')
        return branches_obj
    except Exception as err:
        logger.error(err)


async def get_specs(message: types.Message, branch: str):
    # Идентификатор филиала передаем сюда явно, чтобы не гадать, откуда он взялся
    #   Пусть даже и пустой
    try:
        await bot.send_chat_action(chat_id=message.chat.id, action='typing')
        get_specs_data = {'chatid': message.chat.id,
                          # 'action': 'spec_list',
                          'qqc244': branch}
        # r = requests.post(f'{url}/spec_list/', get_specs_data)
        # r = bytes(r.text.encode('utf-8'))
        # specs = msgspec.json.decode(r)
        # r = ('"result":"success"}')
        link = f'{url}spec_list/'
        decoded_data = await decode_data(message, link, get_specs_data)
        specs = decoded_data['spec']
        return specs
    except Exception as err:
        logger.error(err)


# async def trigger_callback_query_handler(message):
#     # create a fake callback query object with the desired data
#     callback_query = types.CallbackQuery(
#         message=message,
#         data="datetime_" + user_choices[message.chat.id]['day']
#     )
#     # data="date_time_custom"
#     # pass the fake callback query object to the callback query handler
#     await process_slots_by_specs(callback_query, None)


async def post_appointment(message: types.Message):
    error = None
    await bot.send_chat_action(chat_id=message.chat.id, action='typing')
    try:
        if 'qqc_pat' in user_choices[message.chat.id]:
            form_data = {'chatid': message.chat.id,
                         'patQQC': user_choices[message.chat.id]['qqc_pat'],
                         'doc': user_choices[message.chat.id]['doc_qqc'],
                         'date': user_choices[message.chat.id]['day'],
                         'time': user_choices[message.chat.id]['time']
                         }
            # r = requests.post(f'{url}/appointByQQC/', form_data)
            # r = bytes(r.text.encode('utf-8'))
            # r = msgspec.json.decode(r)
            # r = ('"result":"success"}')
            link = f'{url}appointByQQC/'
            decoded_data = await decode_data(message, link, form_data)
        else:
            form_data = {'chatid': message.chat.id,
                         # 'action': 'appointByFIO',
                         'phone': user_choices[message.chat.id]['phone'],
                         'fio': user_choices[message.chat.id]['fio'],
                         'birthdate': user_choices[message.chat.id]['birthdate'],
                         'doc': user_choices[message.chat.id]['doc_qqc'],
                         'date': user_choices[message.chat.id]['day'],
                         'time': user_choices[message.chat.id]['time']
                         }
            # r = requests.post(f'{url}/appointByFIO/', form_data)
            # r = bytes(r.text.encode('utf-8'))
            # r = msgspec.json.decode(r)
            # r = ('"result":"success"}')
            link = f'{url}appointByFIO/'
            decoded_data = await decode_data(message, link, form_data)
        if decoded_data['result'] == 'success':
            logger.info(
                f"{message.chat.id} {message.chat.first_name} записал {user_choices[message.chat.id]['fio']} ({user_choices[message.chat.id]['birthdate']}) к специалисту {user_choices[message.chat.id]['doc_fio']} ({user_choices[message.chat.id]['doc_qqc']}) ({user_choices[message.chat.id]['spec']}) на {user_choices[message.chat.id]['day']} {user_choices[message.chat.id]['time']}")

            qqc153 = decoded_data['qqc153']
            qqc1860 = decoded_data['qqc1860']
            try:
                sql = "insert into appointments (chatID, patientQQC, appointmentQQC, date, time) values (%s,%s,%s,%s,%s)"
                val = (message.chat.id, qqc153, qqc1860, user_choices[message.chat.id]['day'],
                       user_choices[message.chat.id]['timeShort'])
                mycursor.execute(sql, val)
                mydb.commit()
            except mysql.connector.OperationalError as e:
                logger.error('error in connection to database: '.format(e))
                logger.warning("try to reconnect to db")
                mydb.reconnect(3, 2)
            except mysql.connector.Error as e:
                logger.error('error in insert mid.'.format(e))
            except Exception as err:
                logger.error(err)

            if decoded_data['note'] != "":
                note = f"\n☝🏻{decoded_data['note']}☝🏻"
            else:
                note = ""
            await bot.send_message(message.chat.id, f'Вы успешно записаны{note}')
            await user_data_saving(message.chat.id,
                                   user_choices[message.chat.id]['fio'],
                                   user_choices[message.chat.id]['phone'],
                                   user_choices[message.chat.id]['birthdate'])
            user_choices.pop(message.chat.id, )
            await cmd_start(message)
        else:
            error = decoded_data['error']
            logger.info(
                f"{message.chat.id} {message.chat.first_name} ошибка записи {user_choices[message.chat.id]['fio']} ({user_choices[message.chat.id]['birthdate']}) к специалисту {user_choices[message.chat.id]['doc_fio']} ({user_choices[message.chat.id]['doc_qqc']}) ({user_choices[message.chat.id]['spec']}) на {user_choices[message.chat.id]['day']} {user_choices[message.chat.id]['time']}. {error}")

            raise Exception(decoded_data['error'])

    except Exception as err:
        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Выбрать другое время",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        if error:
            sent_message = await bot.send_message(message.chat.id, str(err), reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
        else:
            sent_message = await bot.send_message(message.chat.id, 'Что-то пошло не так, попробуйте ещё раз.',
                                                  reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
        logger.error(err)


async def get_keyboard(message, param, reply_msg, prefix):
    try:
        if reply_msg == 'Выберите филиал':
            keyboard = types.InlineKeyboardMarkup(row_width=1)
            back_button = types.InlineKeyboardButton(text="ОТМЕНА", callback_data="cancel")
            any_button = types.InlineKeyboardButton(text="ЛЮБОЙ", callback_data=prefix + "any")

            button_list = [types.InlineKeyboardButton(text=key, callback_data=prefix + value) for key, value in
                           param.items()]
            keyboard.add(*button_list, any_button, back_button)
            sent_message = await bot.send_message(message.chat.id, reply_msg, reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
        else:
            back_button = types.InlineKeyboardButton(text="<-- Назад", callback_data='back_' + prefix)
            if prefix.startswith("doctime_"):
                # Оставим временно ширину клавиатуры 1, т.к. иначе ломается определение нажатой кнопки в функции
                # edit_message_with_choises т.к. судя по всему клавиатура становится массивом массивов, а не плоским
                # массивом :( keyboard = types.InlineKeyboardMarkup(row_width=3, one_time_keyboard=True)
                keyboard = types.InlineKeyboardMarkup(row_width=3, one_time_keyboard=True)
            elif prefix == 'datetime_' or prefix.startswith('datetime_bydoc_'):
                keyboard = types.InlineKeyboardMarkup(row_width=2, one_time_keyboard=True)
            else:
                keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
            if prefix == 'datetime_' or prefix.startswith('doctime_') or prefix.startswith('datetime_bydoc_'):
                button_list = [types.InlineKeyboardButton(text=value, callback_data=prefix + key) for key, value in
                               param.items()]
                keyboard.add(*button_list, back_button)
            elif prefix == 'savedusers_':
                button_list = [types.InlineKeyboardButton(text=x, callback_data=prefix + str(i)) for i, x in
                               enumerate(param)]
                any_button = types.InlineKeyboardButton(text="Записать другого человека",
                                                        callback_data='savedusers_Другого')
                keyboard.add(*button_list, any_button, back_button)
            elif prefix == 'spec_':
                button_list = [types.InlineKeyboardButton(text=x, callback_data=f'{prefix}{str(i)}') for i, x in
                               enumerate(param)]
                keyboard.add(*button_list, back_button)
            elif prefix == 'docprice_':
                button_list = [types.InlineKeyboardButton(text=f'{x.get("fio")}: {x.get("price")} руб.',
                                                          callback_data=f'{prefix}{x.get("qqc")}') for x in
                               param]
                if param:
                    any_button = types.InlineKeyboardButton(text="ЛЮБОЙ", callback_data=f'docprice_any')
                    if len(param) > 1:
                        keyboard.add(any_button, *button_list, back_button)
                    else:
                        keyboard.add(*button_list, back_button)
                else:
                    keyboard.add(back_button)
                    reply_msg = 'К сожалению нет доступных врачей в ближайшее время.'
            else:
                button_list = [types.InlineKeyboardButton(text=x, callback_data=prefix + x) for x in param]
                keyboard.add(*button_list, back_button)
            sent_message = await bot.send_message(chat_id=message.chat.id, text=str(reply_msg),
                                                  reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
    except Exception as err:
        logger.error(err)


async def get_slots(message: types.Message, spec: str, qqc244branch: str = "", day: str = "", qqc244: str = ""):
    try:
        get_slots_by_spec_data = {'chatid': message.chat.id,
                                  # 'action': 'getslotsbyspec',
                                  'spec': spec
                                  # 'day': day
                                  }
        if qqc244branch != "":
            get_slots_by_spec_data['qqc244branch'] = qqc244branch
        if day != "":
            get_slots_by_spec_data['day'] = day
        if qqc244 == "":
            get_slots_by_spec_data['qqc244'] = ""
        elif qqc244 != "false":
            get_slots_by_spec_data['qqc244'] = qqc244
        # else:
        #     get_slots_by_spec_data['qqc244'] = qqc244
        await bot.send_chat_action(chat_id=message.chat.id, action='typing')
        # r = requests.post(f'{url}/getslotsbyspec/', get_slots_by_spec_data)
        # r = bytes(r.text.encode('utf-8'))
        # slots = msgspec.json.decode(r)
        # r = ('"result":"success"}')
        link = f'{url}getslotsbyspec/'
        decoded_data = await decode_data(message, link, get_slots_by_spec_data)
        return decoded_data
    except Exception as err:
        logger.error(err)


async def get_doc_det(message, doc_qqc: str):
    try:
        # Получим детали (ФИО и имя филиала) о специалисте по его qqc
        req = {'chatid': message.chat.id,
               # 'action': 'getdocinfo',
               'qqc_doc': doc_qqc}
        # r = requests.post(f'{url}/getdocinfo/', req)
        # r = bytes(r.text.encode('utf-8'))
        # doc_info = msgspec.json.decode(r)
        # r = ('{"result":"success","branches":{"\u0000":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f '
        #      '\u043f\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 (\u0443\u043b. '
        #      '40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
        #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC",'
        #      '"orgqqc":"\u0422ACdAAFAqAA"},"\u0422ACdAAn"}')
        link = f'{url}getdocinfo/'
        decoded_data = await decode_data(message, link, req)
        return decoded_data["doc"]
    except Exception as err:
        logger.error(err)


async def edit_message_with_choices(cbq, text=""):
    try:
        text_to_show = ''
        for btn_l1 in cbq.message.reply_markup.inline_keyboard:
            for btn_l2 in btn_l1:
                if btn_l2.callback_data == cbq.data:
                    if text == "":
                        text_to_show = "Нажали: " + btn_l2.text
                    else:
                        text_to_show = text
        sql = "SELECT chatID,msgID FROM users WHERE chatID = %s ORDER BY msgID"
        chat_id = (cbq.message.chat.id,)
        mycursor.execute(sql, chat_id)
        myresult = mycursor.fetchall()
        total_messages_to_process = len(myresult)
        for i in range(total_messages_to_process):
            x = myresult[i]
            # logger.info(f"Сообщение {i} из {total_messages_to_process}")
            try:
                if i < total_messages_to_process - 1:
                    await bot.delete_message(chat_id=cbq.message.chat.id, message_id=x[1])
                else:
                    await bot.edit_message_text(chat_id=cbq.message.chat.id, message_id=x[1], text=text_to_show)
            except MessageCantBeDeleted:
                await bot.edit_message_reply_markup(cbq.message.chat.id, x[1], reply_markup=None)
                await bot.edit_message_text(chat_id=cbq.message.chat.id, message_id=x[1], text='Отменено')
            except MessageNotModified as e:
                # logger.error(e)
                logger.error(
                    f"Вылезла ошибка '{e}' от телеги, что сообщение {x[1]} в чате {cbq.message.chat.id} {cbq.message.chat.first_name} не изменилось. Но мы это проигнорим.")
            except MessageToEditNotFound or MessageToDeleteNotFound as e:
                logger.error(
                    f"Вылезла ошибка '{e}' от телеги на редактирование сообщения {x[1]} в чате {cbq.message.chat.id} {cbq.message.chat.first_name}. Но мы это проигнорим.")
            except Exception as e:
                logger.error(
                    f"Вылезла еще какая-то ошибка от телеги на редактирование сообщения {x[1]} в чате {cbq.message.chat.id} {cbq.message.chat.first_name}: '{e}'. Но мы это проигнорим.")
            finally:
                sql = "delete from users where chatID=%s and msgID=%s"
                val = (cbq.message.chat.id, x[1])
                mycursor.execute(sql, val)
                mydb.commit()

    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as err:
        logger.error('error while updating messages with choises: ', err)
    except Exception as err:
        logger.error(err)


async def check_db_connection():
    if mydb.is_connected():
        return True
    else:
        logger.error('error in connection to database')
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)


async def get_pressed_button_text(cbq: types.CallbackQuery):
    try:
        # Функция получения текста нажатой callback button
        for btn_l1 in cbq.message.reply_markup.inline_keyboard:
            for btn_l2 in btn_l1:
                if btn_l2.callback_data == cbq.data:
                    return btn_l2.text
    except Exception as err:
        logger.error(err)


async def get_users_info(message: types.Message):
    try:
        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Отмена",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        await UsersData.fio.set()
        sent_message = await bot.send_message(message.chat.id,
                                              'Пожалуйста, в строку ввода внесите полные ФИО записываемого пациента, '
                                              'используя пробел (более в ячейку ничего не вписывать)',
                                              reply_markup=keyboard)
        await insert_mid(message.chat.id, sent_message.message_id)
    except Exception as err:
        logger.error(err)


@dp.message_handler(state=UsersData.fio)
async def process_fio(message: types.Message, state: FSMContext):
    try:
        mid = await check_mid(message.chat.id)
        await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
        sql = "delete from users where chatID=%s and msgID=%s"
        val = (message.chat.id, mid[1])
        mycursor.execute(sql, val)
        mydb.commit()
        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Отмена",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        if message.text == 'Записаться к врачу':
            logger.info(f"{message.chat.id} {message.chat.first_name} ввел некорректное ФИО: {message.text}")

            sent_message = await bot.send_message(message.chat.id, 'Вы ввели некорректное ФИО, попробуйте ещё раз',
                                                  reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
            await state.set_state(UsersData.fio.state)
        else:
            await add_to_form(message.chat.id, 'fio', message.text)
            logger.info(
                f"{message.chat.id} {message.chat.first_name} ввел ФИО пациента: {user_choices[message.chat.id]['fio']}")

            await UsersData.next()
            sent_message = await bot.send_message(message.chat.id, "Пожалуйста, введите дату рождения в формате "
                                                                   "ДД.ММ.ГГГГ",
                                                  reply_markup=keyboard)

        await insert_mid(message.chat.id, sent_message.message_id)
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as err:
        logger.error(err)
    except Exception as err:
        logger.error(err)


@dp.message_handler(state=UsersData.phone_number)
async def process_phone_number(message: types.Message, state: FSMContext):
    try:
        mid = await check_mid(message.chat.id)
        sql = "delete from users where chatID=%s and msgID=%s"
        val = 0
        if not mid:
            pass
        elif len(mid) > 1:
            await bot.edit_message_reply_markup(message.chat.id, mid[len(mid) - 1], reply_markup=None)
            val = (message.chat.id, mid[len(mid) - 1])
            mycursor.execute(sql, val)
            mydb.commit()
        else:
            await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
            val = (message.chat.id, mid[1])
            mycursor.execute(sql, val)
            mydb.commit()

        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Отмена",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        phone = re.sub(r'\D', '', message.text)
        if len(phone) != 11:
            logger.info(f"{message.chat.id} {message.chat.first_name} ввел некорректный номер телефона: {message.text}")

            sent_message = await bot.send_message(message.chat.id, 'Вы ввели некорректный номер, попробуйте ещё раз',
                                                  reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
            await state.set_state(UsersData.phone_number.state)
        else:
            user_choices[message.chat.id]['phone'] = phone
            logger.info(
                f"{message.chat.id} {message.chat.first_name} отправляем sms для проверки номера: {phone}")
            response = await send_sms(message, phone)
            if response['result'] == 'success':
                logger.info(
                    f"{message.chat.id} {message.chat.first_name} sms отправлена на номер: {phone}")
                # user_choices[message.chat.id]['phone'] = phone
                await UsersData.next()
                sent_message = await bot.send_message(message.chat.id,
                                                      "Нужно проверить ваш телефонный номер, сейчас вам прийдет смс с кодом для подтверждения, введите код",
                                                      reply_markup=keyboard)

                await insert_mid(message.chat.id, sent_message.message_id)
            else:
                logger.info(
                    f"{message.chat.id} {message.chat.first_name} ошибка отправки sms ({response['error']}) на номер: {phone}")
                if response['error'] == "Код уже отправлен.":
                    sent_message = await bot.send_message(message.chat.id,
                                                          "Код для проверки этого номера уже отправлен, введите его:",
                                                          reply_markup=keyboard)
                    await UsersData.next()
                else:
                    await state.finish()
                    sent_message = await bot.send_message(message.chat.id,
                                                          response['error']
                                                          )

                await insert_mid(message.chat.id, sent_message.message_id)
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as err:
        logger.error(err)
    except Exception as err:
        logger.error(err)


async def send_sms(message: types.Message, phone):
    check_phone = {'chatid': message.chat.id,
                   'phone': phone
                   }
    # r = requests.post(f'{url}/phone_check/', check_phone)
    # r = bytes(r.text.encode('utf-8'))
    # r = msgspec.json.decode(r)
    # r = ('{"result":"success","branches":{"\u0000":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f '
    #      '\u043f\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 (\u0443\u043b. '
    #      '40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
    #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC",'
    #      '"orgqqc":"\u0422ACdAAFAqAA"},"\u0422ACdAAn"}')
    link = f'{url}phone_check/'
    decoded_data = await decode_data(message, link, check_phone)
    return decoded_data


@dp.message_handler(state=UsersData.phone_number_validation)
async def process_phone_validation(message: types.Message, state: FSMContext):
    try:
        f"{message.chat.id} {message.chat.first_name} выполняется проверка номера: {user_choices[message.chat.id]['phone']}"
        mid = await check_mid(message.chat.id)
        sql = "delete from users where chatID=%s and msgID=%s"
        val = 0
        if not mid:
            pass
        elif len(mid) > 1:
            await bot.edit_message_reply_markup(message.chat.id, mid[len(mid) - 1], reply_markup=None)
            val = (message.chat.id, mid[len(mid) - 1])
            mycursor.execute(sql, val)
            mydb.commit()
        else:
            await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
            val = (message.chat.id, mid[1])
            mycursor.execute(sql, val)
            mydb.commit()

        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Отмена",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        check_code = {'chatid': message.chat.id,
                      'phone': user_choices[message.chat.id]['phone'],
                      'code': message.text
                      }
        # r = requests.post(f'{url}/check_code/', check_code)
        # r = bytes(r.text.encode('utf-8'))
        # r = msgspec.json.decode(r)
        # r = ('{"result":"success","branches":{"\u0000":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f '
        #      '\u043f\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 (\u0443\u043b. '
        #      '40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
        #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC",'
        #      '"orgqqc":"\u0422ACdAAFAqAA"},"\u0422ACdAAn"}')
        link = f'{url}check_code/'
        decoded_data = await decode_data(message, link, check_code)
        if decoded_data['result'] == 'success':
            logger.info(
                f"{message.chat.id} {message.chat.first_name} проверка номера: {user_choices[message.chat.id]['phone']} прошла успешно")
            await state.finish()
            await reasuring(message)
        else:
            logger.info(
                f"{message.chat.id} {message.chat.first_name} проверка номера: {user_choices[message.chat.id]['phone']} - введен неправильный код")

            response = await send_sms(message, user_choices[message.chat.id]['phone'])
            if response['result'] == 'success':
                sent_message = await bot.send_message(message.chat.id,
                                                      'Вы ввели некорректный код, будет отправлен новый, попробуйте ещё раз.',
                                                      reply_markup=keyboard)
                await insert_mid(message.chat.id, sent_message.message_id)
                await state.set_state(UsersData.phone_number_validation.state)
            else:
                await state.set_state(UsersData.phone_number.state)
                sent_message = await bot.send_message(message.chat.id,
                                                      response['error'],
                                                      reply_markup=keyboard)

                await insert_mid(message.chat.id, sent_message.message_id)
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as err:
        logger.error(err)
    except Exception as err:
        logger.error(err)


@dp.message_handler(state=UsersData.birth_date)
async def process_birth_date(message: types.Message, state: FSMContext):
    try:
        keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
        back_btn = types.InlineKeyboardButton(text="Отмена",
                                              callback_data='datetime_' + user_choices[message.chat.id]['day'])
        keyboard.add(back_btn)
        mid = await check_mid(message.chat.id)
        sql = "delete from users where chatID=%s and msgID=%s"
        val = 0
        if not mid:
            pass
        elif len(mid) > 1:
            await bot.edit_message_reply_markup(message.chat.id, mid[len(mid) - 1], reply_markup=None)
            val = (message.chat.id, mid[len(mid) - 1])
            mycursor.execute(sql, val)
            mydb.commit()
        else:
            await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
            val = (message.chat.id, mid[1])
            mycursor.execute(sql, val)
            mydb.commit()

        birthdate = ''
        if len(message.text) == 8 or len(message.text) == 10:
            try:
                birthdate = datetime.strptime(message.text, "%d%m%Y")
                birthdate = datetime.strftime(birthdate, "%d.%m.%Y")
            except Exception:
                birthdate = datetime.strptime(message.text, "%d.%m.%Y")
                birthdate = datetime.strftime(birthdate, "%d.%m.%Y")
            await add_to_form(message.chat.id, 'birthdate', birthdate)
            logger.info(f"{message.chat.id} {message.chat.first_name} ввел дату рождения: {birthdate}")

            search_patient = {'chatid': message.chat.id,
                              'fio': user_choices[message.chat.id]['fio'],
                              'birthdate': user_choices[message.chat.id]['birthdate']
                              }
            # r = requests.post(f'{url}/searchPatientByFIO/', search_patient)
            # r = bytes(r.text.encode('utf-8'))
            # r = msgspec.json.decode(r)
            # r = ('{"result":"success","branches":{"\u0000":{"title":"\u0412\u0437\u0440\u043e\u0441\u043b\u0430\u044f '
            #      '\u043f\u043e\u043b\u0438\u043a\u043b\u0438\u043d\u0438\u043a\u0430 (\u0443\u043b. '
            #      '40-\u043b\u0435\u0442\u0438\u044f \u041f\u043e\u0431\u0435\u0434\u044b, 11)",'
            #      '"org":"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a","qqc":"\u0422ACdAAC",'
            #      '"orgqqc":"\u0422ACdAAFAqAA"},"\u0422ACdAAn"}')
            link = f'{url}searchPatientByFIO/'
            decoded_data = await decode_data(message, link, search_patient)
            if decoded_data['patient']['found'] == 0 or decoded_data['patient']['found'] > 1:
                sent_message = await bot.send_message(message.chat.id,
                                                      "Пожалуйста, введите телефонный номер записываемого как 11 цифр без "
                                                      "пробелов и других символов", reply_markup=keyboard)
                await insert_mid(message.chat.id, sent_message.message_id)
                await UsersData.next()
            elif decoded_data['patient']['found'] == 1:
                await add_to_form(message.chat.id, 'qqc_pat', decoded_data['patient']['qqc_pat'])
                phone = str(decoded_data['patient']['phone']).replace('_', '')

                await add_to_form(message.chat.id, 'phone', phone)
                await state.finish()
                await reasuring(message)
        else:
            sent_message = await bot.send_message(message.chat.id,
                                                  'Вы ввели некорректную дату рождения, попробуйте ещё раз',
                                                  reply_markup=keyboard)
            await insert_mid(message.chat.id, sent_message.message_id)
            logger.info(f"{message.chat.id} {message.chat.first_name} ввел некорректную дату рождения: {message.text}")

            await state.set_state(UsersData.birth_date.state)

    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as err:
        logger.error(err)
    except Exception as err:
        logger.error(err)


async def reasuring(message: types.Message):
    try:
        if user_choices[message.chat.id]["doc_filial"]:
            filial = f'Филиал: {user_choices[message.chat.id]["doc_filial"]}'
        else:
            filial = ''

        # Сделаем запрос на получение примечаний к записи. На базе всех данных, что мы уже ввели
        # Не используем, оставлю как пример запроса, вдруг в будущем пригодится.
        # Код рабочий, проверил на тесте.
        # notesRequestData = {'chatid': message.chat.id,
        #                     'user_choises': msgspec.json.encode(user_choices),
        #                     'fio': user_choices[message.chat.id]['fio'],
        #                     'birthdate': user_choices[message.chat.id]['birthdate']
        #                     }
        # r = requests.post(f'{url}getNotesForAppointment/', notesRequestData)
        # r = bytes(r.text.encode('utf-8'))
        # r = msgspec.json.decode(r)
        # note = ""
        # if r['result'] == 'success':
        #     if r['notes'] != "":
        #         note = f"\n{r['notes']}"
        # mess = f'<b>Чтобы подтвердить запись, проверьте данные и НАЖМИТЕ КНОПКУ "ПОДТВЕРЖДАЮ" ниже:</b>\n\nЗаписываем: {user_choices[message.chat.id]["fio"]}\nДата рождения: {user_choices[message.chat.id]["birthdate"]}\nТелефон: {user_choices[message.chat.id]["phone"]}\nк специалисту: {user_choices[message.chat.id]["doc_fio"]} ({user_choices[message.chat.id]["spec"]})\nна {user_choices[message.chat.id]["timeShort"]} {user_choices[message.chat.id]["dayName"]}\nСтоимость приема: {prices.get(user_choices[message.chat.id]["doc_qqc"])} руб.\n{filial}{note}'
        # data = user_choices[message.chat.id]
        data = str(json.dumps(user_choices[message.chat.id])).encode('utf-8', 'strict')
        get_specs_data = {'chatid': message.chat.id,
                          # 'action': 'spec_list',
                          'user_choises': data}
        # r = requests.post(f'{url}/spec_list/', get_specs_data)
        # r = bytes(r.text.encode('utf-8'))
        # specs = msgspec.json.decode(r)
        # r = ('"result":"success"}')
        link = f'{url}getnotesforappointment/'
        decoded_data = await decode_data(message, link, get_specs_data)

        mess = f'<b>Чтобы подтвердить запись, проверьте данные и НАЖМИТЕ КНОПКУ "ПОДТВЕРЖДАЮ" ниже:</b>\n\nЗаписываем: {user_choices[message.chat.id]["fio"]}\nДата рождения: {user_choices[message.chat.id]["birthdate"]}\nТелефон: {user_choices[message.chat.id]["phone"]}\nк специалисту: {user_choices[message.chat.id]["doc_fio"]} ({user_choices[message.chat.id]["spec"]})\nна {user_choices[message.chat.id]["timeShort"]} {user_choices[message.chat.id]["dayName"]}\nСтоимость приема: {prices.get(user_choices[message.chat.id]["doc_qqc"])} руб.\n{filial}'
        if decoded_data['result'] == 'success' and decoded_data['notes'] != '':
            mess = f'{mess}\n\n{decoded_data["notes"]}'

        keyboard = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="ПОДТВЕРЖДАЮ ✅", callback_data='yes'),
                    InlineKeyboardButton(text="Отмена", callback_data='no')
                ],
                [
                    InlineKeyboardButton(text="<-- Назад",
                                         callback_data=f'backdoctime_{user_choices[message.chat.id]["doc_qqc"]}_{user_choices[message.chat.id]["time"]}')
                ]
            ]
        )
        sent_message = await bot.send_message(chat_id=message.chat.id, text=mess, parse_mode='html',
                                              reply_markup=keyboard)
        await insert_mid(message.chat.id, sent_message.message_id)
        logger.info(f'{message.chat.id} {message.chat.first_name}: отправили пользователю запрос на подтверждение записи.')
    except KeyError as err:
        logger.error(f'Ошбика KeyError {err}')
        await bot.send_message(message.chat.id,
                               text=f'<b>К сожалению что-то пошло не так, попробуйте ещё раз.</b>', parse_mode='html')
        await make_appointment(message)
    except Exception as err:
        logger.error(err)
        await bot.send_message(message.chat.id,
                               text=f'<b>К сожалению что-то пошло не так, попробуйте ещё раз.</b>', parse_mode='html')
        await make_appointment(message)


async def check_mid(cid):
    try:
        sql = "SELECT chatID,msgID FROM users WHERE chatID = %s"
        chat_id = (cid,)

        mycursor.execute(sql, chat_id)

        myresult = mycursor.fetchall()
        for x in myresult:
            return x
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as e:
        logger.error('error in check_mid, ', e)
    except Exception as err:
        logger.error(err)


async def insert_mid(cid, mid):
    try:
        if mid is None:
            sql = "delete from users where chatID=%s"
            val = (cid,)
        else:
            sql = "insert into users (chatID, msgID) values (%s,%s)"
            val = (cid, mid)
        mycursor.execute(sql, val)
        mydb.commit()
        return 'success'

    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as e:
        logger.error('error in insert mid.', e)
    except Exception as err:
        logger.error(err)


@dp.message_handler(
    lambda message: any(
        word in message.text.lower() for word in dict_to_doctor))
async def make_appointment(message: types.Message):
    logger.info(f"{message.chat.id} {message.chat.first_name} нажал записаться к врачу.")
    try:
        # Для начала почистим старые сообщения с вариантами выбора (аналог edit_message_with_choices)
        # Не знаю, нафига, но зачем-то написали здесь это снова. TODO Надо бы сделать красиво. Написали еще раз потому, что здесь объект message, а не cbq
        sql = "SELECT chatID,msgID FROM users WHERE chatID = %s"
        chat_id = (message.chat.id,)
        mycursor.execute(sql, chat_id)
        myresult = mycursor.fetchall()
        if myresult:
            total_messages_to_proccess = len(myresult)
            for i in range(len(myresult)):
                mid = myresult[i][1]
                try:
                    if i < total_messages_to_proccess - 1:
                        logger.info(f"Удаляем в чате {message.chat.id} {message.chat.first_name} сообщение {mid}.")
                        await bot.delete_message(message.chat.id, mid)
                    else:
                        logger.info(
                            f"Удаляем в чате {message.chat.id} {message.chat.first_name} в сообщении {mid} кнопки и правим текст на 'отменено'.")
                        await bot.edit_message_reply_markup(message.chat.id, mid, reply_markup=None)
                        await bot.edit_message_text(chat_id=message.chat.id, message_id=mid, text='Отменено')
                except MessageCantBeDeleted as e:
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги, что сообщение {mid} в чате {message.chat.id} {message.chat.first_name} удалить нельзя. Попробуем отредактировать его.")
                    try:
                        logger.info(
                            f"Удаляем в чате {message.chat.id} {message.chat.first_name} в сообщении {mid} кнопки и правим текст на 'отменено'.")
                        await bot.edit_message_reply_markup(message.chat.id, mid, reply_markup=None)
                        await bot.edit_message_text(chat_id=message.chat.id, message_id=mid, text='Отменено')
                    except Exception as e:
                        logger.error(
                            f"Вылезла еще какая-то ошибка от телеги на редактирование сообщения {mid} в чате {message.chat.id} {message.chat.first_name} (после неудачной попытки удаления сообщения): '{e}'. Но мы это проигнорим.")
                except MessageNotModified as e:
                    # logger.error(e)
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги, что сообщение {mid} в чате {message.chat.id} {message.chat.first_name} не изменилось. Но мы это проигнорим.")
                except MessageToEditNotFound or MessageToDeleteNotFound as e:
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги на редактирование сообщения {mid} в чате {message.chat.id} {message.chat.first_name}. Но мы это проигнорим.")
                except Exception as e:
                    logger.error(
                        f"Вылезла еще какая-то ошибка от телеги на редактирование сообщения {mid} в чате {message.chat.id} {message.chat.first_name}: '{e}'. Но мы это проигнорим.")
                finally:
                    # await bot.edit_message_text(chat_id=message.chat.id, message_id=mid, text='Отменено')
                    # await insert_mid(message.chat.id, None)             # Потенциально это жопа, т.к. мы удаляем из базы все msgID для этого chatID. И это при обработке только одного из них!
                    # Но пока что "и так сойдет", потому что мы все msgID уже имеем в myresult и по любому по ним по всем пройдемся
                    # Тогда лажа только в том, что сколько было сообщений, столько раз мы попытаемся из базы удалить все строки для этого chatID
                    # await get_keyboard(message, await get_branches(message), 'Выберите филиал', 'branch_')
                    sql = "delete from users where chatID=%s and msgID=%s"
                    val = (message.chat.id, mid)
                    mycursor.execute(sql, val)
                    mydb.commit()
        # else:
        #     logger.info(f"{message.chat.id} (no msgID) {message.chat.first_name} нажал записаться к врачу.")

        # Ну и наконец-то выведем пользователю список филиалов на выбор
        branches = await get_branches(message)
        if branches:
            await get_keyboard(message, branches, 'Выберите филиал', 'branch_')
        else:
            await add_to_form(message.chat.id, 'qqc244branch', '')
            await add_to_form(message.chat.id, 'qqc244branchname', 'любой')
            await get_keyboard(message,
                               await get_specs(message,
                                               user_choices[message.chat.id]['qqc244branch']),
                               'Выберите специализацию', 'spec_')
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as e:
        logger.error('error in check_mid, ', e)
    except Exception as err:
        logger.error(err)


@dp.message_handler()
async def any_text_recognition(message: types.Message):
    mid = await check_mid(message.chat.id)
    if mid:
        try:
            await bot.edit_message_reply_markup(message.chat.id, mid[1], reply_markup=None)
            await bot.edit_message_text(chat_id=message.chat.id, message_id=mid[1], text='Отменено')
            await insert_mid(message.chat.id, None)
        except Exception as e:
            logger.error(
                f"Вылезла какая-то ошибка от телеги на редактирование сообщения в чате {message.chat.id} {message.chat.first_name}: '{e}'. Но мы это проигнорим.")
    keyboard = types.InlineKeyboardMarkup(row_width=1, one_time_keyboard=True)
    button = types.InlineKeyboardButton(text="ЗАПИСАТЬСЯ К ВРАЧУ", callback_data="Записаться")
    keyboard.add(button)
    sent_message = await bot.send_message(message.chat.id,
                                          "Я бот, и, к сожалению, пока не умею отвечать на вопросы. Могу записать вас "
                                          "к врачу. Для этого нажмите кнопку под сообщением",
                                          reply_markup=keyboard)
    await insert_mid(message.chat.id, sent_message.message_id)


async def user_data_saving(cid, fio, phone, birthdate):
    try:
        sql = "SELECT * FROM users_info WHERE chatID = %s and fio = %s and birthdate = %s"
        val = (cid, fio, birthdate)
        mycursor.execute(sql, val)
        myresult = mycursor.fetchall()
        if not myresult:
            sql = "insert into users_info (chatID, fio, phone, birthdate) values (%s,%s,%s,%s)"
            val = (cid, fio, phone, birthdate)
            mycursor.execute(sql, val)
            mydb.commit()
            return True
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as e:
        logger.error('error in user_data_saving,', e)
    except Exception as err:
        logger.error(err)


async def user_data_pulling(cid):
    try:
        sql = "SELECT * FROM users_info WHERE chatID = %s"
        chat_id = (cid,)

        mycursor.execute(sql, chat_id)

        myresult = mycursor.fetchall()
        return myresult
    except mysql.connector.OperationalError as e:
        logger.error('error in connection to database: '.format(e))
        logger.warning("try to reconnect to db")
        mydb.reconnect(3, 2)
    except mysql.connector.Error as e:
        logger.error('error in user_data_pulling, ', e)
    except Exception as err:
        logger.error(err)

async def check_db_connection(level=1):
    logger.debug(f"Проверка подключения к базе level={level}. Состяние: {mydb.is_connected()}")
    if mydb.is_connected():
        return True
    else:
        logger.error('error in connection to database')
        logger.warning(f"try to reconnect to db (level={level})")
        mydb.reconnect(3, 2)
        if level<3:
            return await check_db_connection(level+1)
        else: 
            # Если мы погрузились в эту функцию больше чем на 2 уровня - перестаем погружаться глубже и просто возвращаем результат проверки подключения
            logger.info(f"Не идем глубже в попытки переподключения к базе (level={level}), просто возвращаем текущее состояние подключения: {mydb.is_connected()}")
            return mydb.is_connected()


async def edit_messages():
    if await check_db_connection():
        sql = "SELECT chatID,msgID FROM users"
        mycursor.execute(sql, )
        myresult = mycursor.fetchall()
        if myresult:
            total_messages_to_proccess = len(myresult)
            for i in range(len(myresult)):
                mid = myresult[i][1]
                cid = myresult[i][0]
                try:
                    if i < total_messages_to_proccess - 1:
                        # logger.info(f"Удаляем в чате {myresult[i]} сообщение {mid}.")
                        # await bot.delete_message(cid, mid)
                        logger.info(f"Удаляем в чате {myresult[i]} в сообщении {mid} кнопки и правим текст на 'истек таймаут выбора'.")
                        await bot.edit_message_reply_markup(cid, mid, reply_markup=None)
                        await bot.edit_message_text(chat_id=cid, message_id=mid, text='Истек таймаут ожидания выбора.')
                    else:
                        logger.info(f"Удаляем в чате {myresult[i]} в сообщении {mid} кнопки и правим текст на 'истек таймаут выбора'.")
                        await bot.edit_message_reply_markup(cid, mid, reply_markup=None)
                        await bot.edit_message_text(chat_id=cid, message_id=mid, text='Истек таймаут ожидания выбора.')
                except MessageCantBeDeleted as e:
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги, что сообщение {mid} в чате {myresult[i]} удалить нельзя. "
                        f"Попробуем отредактировать его.")
                    try:
                        logger.info(f"Удаляем в чате {myresult[i]} в сообщении {mid} кнопки и правим текст на 'истек таймаут выбора'.")
                        await bot.edit_message_reply_markup(cid, mid, reply_markup=None)
                        await bot.edit_message_text(chat_id=cid, message_id=mid, text='Истек таймаут ожидания выбора.')
                    except Exception as e:
                        logger.error(
                            f"Вылезла еще какая-то ошибка от телеги на редактирование сообщения {mid} в чате {myresult[i]} (после неудачной попытки удаления сообщения): '{e}'. Но мы это проигнорим.")
                except MessageNotModified as e:
                    # logger.error(e)
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги, что сообщение {mid} в чате {myresult[i]} не изменилось. Но мы "
                        f"это проигнорим.")
                except MessageToEditNotFound or MessageToDeleteNotFound as e:
                    logger.error(
                        f"Вылезла ошибка '{e}' от телеги на редактирование сообщения {mid} в чате {myresult[i]}. Но мы "
                        f"это проигнорим.")
                except Exception as e:
                    logger.error(
                        f"Вылезла еще какая-то ошибка от телеги на редактирование сообщения {mid} в чате {myresult[i]}: '{e}'. Но мы это проигнорим.")
                finally:
                    # await bot.edit_message_text(chat_id=message.chat.id, message_id=mid, text='Отменено') await
                    # insert_mid(message.chat.id, None)             # Потенциально это жопа, т.к. мы удаляем из базы все
                    # msgID для этого chatID. И это при обработке только одного из них! Но пока что "и так сойдет",
                    # потому что мы все msgID уже имеем в myresult и по любому по ним по всем пройдемся Тогда лажа только
                    # в том, что сколько было сообщений, столько раз мы попытаемся из базы удалить все строки для этого
                    # chatID await get_keyboard(message, await get_branches(message), 'Выберите филиал', 'branch_')
                    sql = "delete from users where chatID=%s and msgID=%s"
                    val = (cid, mid)
                    mycursor.execute(sql, val)
                    mydb.commit()
    else:
        logger.error("edit_messages(): нет работающего подключения к базе данных, ничего не делаем")

async def list_scheduled_tasks(sched: AsyncIOScheduler):
    logger.info(f"Список заданий шедулера:")
    for job in sched.get_jobs():
        logger.info(f"Задание '{job.name}', расписание '{job.trigger}', следующий запуск '{job.next_run_time}'")


# Запуск процесса поллинга новых апдейтов
async def main():
    try:
        logger.info('Бот запустился')
        # Создадим шедулер
        scheduler = AsyncIOScheduler()
        # Запустим задачу проверки подключения к базе данных
        scheduler.add_job(check_db_connection,'interval',hours=1)
        # scheduler.add_job(check_db_connection,'interval',minutes=2)
        # Задачу вывода списка задач
        scheduler.add_job(list_scheduled_tasks,'interval',hours=1,args=[scheduler,])
        # И запустим задачу очистки сообщений, ждущих выбора пользователя, по ночам (чтобы не висели бесконечно)
        scheduler.add_job(edit_messages,'cron',hour="3-4",minute="11")
        # Запустим шедулер
        scheduler.start()
        # При старте бота тоже очищаем все старые сообщения с кнопками выбора, т.к. бот при перезапуске потерял состояния этих кнопок и будут ошибки при нажатия на них
        await edit_messages()
        # Запускаем обработку бота
        await dp.start_polling(bot)
    except Exception as err:
        logger.error(err)


if __name__ == "__main__":
    asyncio.run(main())
