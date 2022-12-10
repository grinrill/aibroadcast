import typing
from datetime import datetime

from aiogram.dispatcher import FSMContext, filters
from aiogram import types, Dispatcher
from aiogram.types import ContentTypes

from . import schedule
from .res import Steps, Button

if typing.TYPE_CHECKING:
    from .broadcast import Broadcast


def register(bc: "Broadcast", *FILTERS):
    dp = bc.dp

    @dp.message_handler(filters.Text(Button.cancel.text), state=Steps, *FILTERS)
    async def cancel(message: types.Message, state: FSMContext):
        await state.reset_data()
        await state.reset_state()
        await message.answer(
            "Рассылка отменена", reply_markup=types.ReplyKeyboardRemove()
        )
        if bc.return_callback:
            await bc.return_callback(message, state)

    @dp.callback_query_handler(
        filters.Text(Button.cancel_callback.callback_data), state="*", *FILTERS
    )
    async def cancel_cb(q: types.CallbackQuery, state: FSMContext):
        await q.answer("")
        await state.reset_data()
        await state.reset_state()
        await q.message.answer(
            "Рассылка отменена", reply_markup=types.ReplyKeyboardRemove()
        )
        if bc.return_callback:
            await bc.return_callback(q.message, state)

    @dp.message_handler(
        filters.Text(Button.finish_content.text), state=Steps.content, *FILTERS
    )
    async def finish_content(message: types.Message, state: FSMContext):
        await Steps.sending_method.set()
        await bc.choose_sending_method(message, state)

    @dp.message_handler(state=Steps.content, content_types=ContentTypes.ANY, *FILTERS)
    async def content(message: types.Message, state: FSMContext):
        m = await bc.content(message, state)
        if m:
            await state.update_data({"last_bot_message_id": m.message_id})

    @dp.message_handler(
        filters.Text(
            [
                Button.sending_method_copy.text,
                Button.sending_method_forward.text,
            ]
        ),
        state=Steps.sending_method,
        *FILTERS,
    )
    async def set_sending_method(message: types.Message, state: FSMContext):
        forward = {
            Button.sending_method_copy.text: False,
            Button.sending_method_forward.text: True,
        }.get(message.text)
        if forward is None:
            await message.answer(
                "Что-то не то. Выберети из клавиатуры ниже:",
                reply_markup=Button.kb_sending_method,
            )
            return
        await state.update_data({"forward": forward})

        if forward:
            await Steps.schedule.set()
            await bc.choose_schedule(message, state)
        else:
            await Steps.web_page_preview.set()
            await bc.choose_web_page_preview(message, state)

    @dp.message_handler(
        filters.Text(
            [
                Button.web_page_preview_yes.text,
                Button.web_page_preview_no.text,
            ]
        ),
        state=Steps.web_page_preview,
        *FILTERS,
    )
    async def set_web_page_preview(message: types.Message, state: FSMContext):
        disable_web_page_preview = {
            Button.web_page_preview_yes.text: False,
            Button.web_page_preview_no.text: True,
        }.get(message.text)
        if disable_web_page_preview is None:
            await message.answer(
                "Что-то не то. Выберети из клавиатуры ниже:",
                reply_markup=Button.kb_web_page_preview,
            )
            return
        await state.update_data({"disable_web_page_preview": disable_web_page_preview})
        await Steps.schedule.set()
        await bc.choose_schedule(message, state)

    @dp.message_handler(
        state=Steps.schedule,
        *FILTERS,
    )
    async def choose_schedule(message: types.Message, state: FSMContext):
        if message.text == Button.schedule_now.text:
            await state.update_data({"schedule": datetime.now()})
            await Steps.preview.set()
            await bc.preview(message, state)
            return

        try:
            t = schedule.parse(message.text)
        except ValueError:
            await message.answer(
                "Ошибка в формате даты. Попробуйте еще раз или выберите выриант из клавиатуры.",
                reply_markup=Button.kb_schedule,
            )
            return

        await state.update_data({"schedule": t})
        await message.answer(
            f"Время установлено: <code>{schedule.format(t)}</code> (через {schedule.format_delta(t-datetime.now())})"
        )
        await Steps.preview.set()
        await bc.preview(message, state)

    @dp.callback_query_handler(
        filters.Text(Button.preview_save.callback_data), state=Steps.preview, *FILTERS
    )
    async def preview_save(q: types.CallbackQuery, state: FSMContext):
        await q.answer("Подготовка рассылки... Подождите минутку")
        await q.message.chat.do("typing")

        try:
            await bc.save(q.message, state)
        except Exception as e:
            await q.message.answer(
                f"Неизвестная ошибка: <code>{str(e)}</code>\nПопробуйте позже или сообщите разработчику"
            )
            raise
        await q.message.delete_reply_markup()
        await state.reset_state()
        await state.reset_data()
        if bc.return_callback:
            await bc.return_callback(q.message, state)

    @dp.callback_query_handler(filters.Text(startswith="bc_st"), state="*", *FILTERS)
    async def status_update(q: types.CallbackQuery, state: FSMContext):
        await q.answer("Обновляю данные...")
        bc_id = int(q.data.split()[-1])
        bc_data = await bc.storage.get_by_id(bc_id)

        text = await bc.status_message(bc_data)
        kb = Button.get_kb_status(bc_data.id)
        await q.message.edit_text(text, reply_markup=kb)

    @dp.message_handler(
        state=Steps,
        *FILTERS,
    )
    async def unhandled(message: types.Message, state: FSMContext):
        await message.answer("Кажется, я тебя не понял. Попробуй еще раз")
