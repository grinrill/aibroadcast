from aiogram import types
from aiogram.dispatcher.filters.state import State, StatesGroup


class Steps(StatesGroup):
    content = State()
    sending_method = State()
    web_page_preview = State()
    when_to_start = State()
    schedule = State()
    preview = State()


def choosen(c, text):
    if not c:
        return text
    return f"✅ {text}"


class Button:
    cancel = types.KeyboardButton("Отменить рассылку")
    kb_cancel = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb_cancel.row(cancel)

    finish_content = types.KeyboardButton("Продолжить")
    kb_content = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb_content.row(finish_content)
    kb_content.row(cancel)

    sending_method_forward = types.KeyboardButton("Пересылкой")
    sending_method_copy = types.KeyboardButton("Копировать")
    kb_sending_method = types.ReplyKeyboardMarkup(
        resize_keyboard=True, one_time_keyboard=True
    )
    kb_sending_method.row(sending_method_forward, sending_method_copy)
    kb_sending_method.row(cancel)

    web_page_preview_yes = types.KeyboardButton("Включить")
    web_page_preview_no = types.KeyboardButton("Отключить")
    kb_web_page_preview = types.ReplyKeyboardMarkup(
        resize_keyboard=True, one_time_keyboard=True
    )
    kb_web_page_preview.row(web_page_preview_yes, web_page_preview_no)
    kb_web_page_preview.row(cancel)

    schedule_now = types.KeyboardButton("Запуск сейчас")
    kb_schedule = types.ReplyKeyboardMarkup(
        resize_keyboard=True, one_time_keyboard=True
    )
    kb_schedule.row(schedule_now)
    kb_schedule.row(cancel)

    preview_change_content = types.InlineKeyboardButton(
        "Изменить контент", callback_data="newbc_p_cc"
    )
    get_preview_sending_forward = lambda c: types.InlineKeyboardButton(
        choosen(c, "Пересылкой"), callback_data="newbc_sm_f"
    )
    get_preview_sending_copy = lambda c: types.InlineKeyboardButton(
        choosen(c, "Копировать"), callback_data="newbc_sm_c"
    )
    preview_change_schedule = types.InlineKeyboardButton(
        "Изменить время запуска", callback_data="newbc_sc"
    )
    preview_save = types.InlineKeyboardButton(
        "ПОДТВЕРДИТЬ РАССЫЛКУ", callback_data="newbc_save"
    )

    @classmethod
    def get_kb_preview(self, forward: bool):
        kb = types.InlineKeyboardMarkup()
        # kb.row(self.preview_change_content)
        # kb.row(
        #     self.get_preview_sending_forward(forward),
        #     self.get_preview_sending_copy(not forward),
        # )
        # kb.row(self.preview_change_schedule)
        kb.row(self.preview_save)
        return kb

    get_status_update = lambda i: types.InlineKeyboardButton(
        "Обновить", callback_data=f"bc_st {i}"
    )

    @classmethod
    def get_kb_status(self, bc_id: int):
        kb = types.InlineKeyboardMarkup()
        kb.row(self.get_status_update(bc_id))
        return kb
