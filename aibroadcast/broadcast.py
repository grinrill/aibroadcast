import aiogram
from aiogram.dispatcher import FSMContext
from aiogram import types

import typing
import time
import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass

from target import Target
import target
from storage import BroadcastDB, Storage
import res
from res import Steps
import schedule
import handler


@dataclass
class StatusMessage:
    chat_id: int
    message_id: int
    updated_at: datetime


class Broadcast:
    bot: aiogram.Bot
    dp: aiogram.Dispatcher
    target: Target
    storage: Storage
    admins: list

    collected_content = dict()
    status_messages = dict()

    copy_delay = 1 / 20
    forward_delay = (60 * 60) / 2000

    sender_task: asyncio.Task

    def __init__(
        self,
        dp: aiogram.Dispatcher,
        target: Target,
        storage: Storage,
        admins: list,
    ):
        self.bot = dp.bot
        self.dp = dp
        self.target = target
        self.storage = storage
        self.admins = set(admins)

    def register(self, *FILTERS):
        return handler.register(self, *FILTERS)

    async def start(self):
        self.sender_task = asyncio.create_task(self.daemon())

    async def new(self, message: types.Message, state: FSMContext) -> types.Message:
        await Steps.content.set()

        return await message.answer(
            """
Отправьте сообщение для рассылки. Это может быть что угодно: текст, стикер, фото. Кнопку-ссылку можно добавить, отформатировав пост через @postbot или @printfbot.
Учтите, что премиум-эмодзи будут работать только в режиме пересылки.

<i>Можно отправить несколько сообщений - бот обработает все.</i>""",
            reply_markup=res.Button.kb_cancel,
        )

    async def collect_content(self, message: types.Message, sec_wait=1) -> list:
        setattr(message, "collected_at", time.time())

        if self.collected_content.get(message.chat.id):
            self.collected_content[message.chat.id].append(message)
            return None

        self.collected_content[message.chat.id] = [message]

        wait = sec_wait
        while wait > 0:
            await asyncio.sleep(wait)
            wait = (
                self.collected_content[message.chat.id][-1].collected_at
                + sec_wait
                - time.time()
            )

        collected = self.collected_content[message.chat.id]
        del self.collected_content[message.chat.id]
        return collected

    async def content(self, message: types.Message, state: FSMContext) -> types.Message:
        data = await state.get_data()
        messages = data.get("messages", [])

        collected = await self.collect_content(message)
        if collected is None:
            return
        messages += [m.to_python() for m in collected]
        await state.update_data({"messages": messages})

        if lbmid := data.get("last_bot_message_id"):
            await message.chat.delete_message(lbmid)

        return await message.answer(
            f"""
Записано сообщений: {len(messages)}. Вы можете отправить еще или нажать кнопку "Продолжить" """,
            reply_markup=res.Button.kb_content,
        )

    async def choose_sending_method(
        self, message: types.Message, state: FSMContext
    ) -> types.Message:
        return await message.answer(
            """
Как отправлять рассылку?

<b>Пересылкой</b> - бот будет пересылать то сообщение, которое вы ему отправили. Если вы переслали пост с канала, то сможете таким образом увидеть охват релкамного поста.
Однако <u>такая рассылка будет идти очень медленно.</u>

<b>Копировать</b> - обычная рассылка, бот будет отправлять сообщения от своего имени. Идет гораздо быстрее, чем рассылка <i>Пересылкой</i>.""",
            reply_markup=res.Button.kb_sending_method,
        )

    async def choose_web_page_preview(
        self, message: types.Message, state: FSMContext
    ) -> types.Message:
        return await message.answer(
            "Включить предпросмотр ссылок?", reply_markup=res.Button.kb_web_page_preview
        )

    async def choose_schedule(
        self, message: types.Message, state: FSMContext
    ) -> types.Message:
        return await message.answer(
            """
Когда запускать рассылку?
Отправьте время запуска рассылки в формате `ДД.ММ чч:мм` или выберете "Запуск сейчас".""",
            reply_markup=res.Button.kb_schedule,
        )

    async def preview(self, message: types.Message, state: FSMContext) -> types.Message:
        data = await state.get_data()
        messages = data["messages"]
        forward = data.get("forward")
        disable_web_page_preview = data.get("disable_web_page_preview")
        schedule_at = data.get("schedule")
        schedule_delta = schedule_at - datetime.now()
        is_scheduled = schedule_delta > timedelta(minutes=1)

        await self.send_messages(
            messages, message.chat.id, forward, disable_web_page_preview
        )
        return await message.answer(
            f"""
👆Предпросмотр рассылки👆

Количество сообщений: {len(messages)}
Способ рассылки: {("копировать", "пересылать")[forward]}
{("Предпросмотр ссылки: " + ("включить", "отключить")[disable_web_page_preview]) if not forward else ""}
Запуск рассылки: <code>{schedule.format(schedule_at)}</code> ({f"через {schedule.format_delta(schedule_delta)})" if is_scheduled else "сейчас"})

Если все верно, нажмите "ПОДТВЕРДИТЬ РАССЫЛКУ"
            """,
            reply_markup=res.Button.get_kb_preview(forward),
        )

    async def save(self, message: types.Message, state: FSMContext) -> types.Message:
        data = await state.get_data()
        messages = data["messages"]
        forward = data.get("forward")
        disable_web_page_preview = data.get("disable_web_page_preview")
        schedule_at = data.get("schedule")
        schedule_delta = schedule_at - datetime.now()
        is_scheduled = schedule_delta > timedelta(minutes=1)

        bc = BroadcastDB(messages, forward, disable_web_page_preview, schedule_at)
        bc = await self.storage.create(bc)
        count = self.target.init(bc.id)

        if forward:
            duration = self.forward_delay * count * len(messages)
        else:
            duration = self.copy_delay * count * len(messages)

        return await message.answer(
            f"""
<b>Создана рассылка #{bc.id}</b>

Количество сообщений: {len(messages)}
Способ рассылки: {("копировать", "пересылать")[forward]}
{("Предпросмотр ссылки: " + ("включить", "отключить")[disable_web_page_preview]) if not forward else ""}
Запуск рассылки: <code>{schedule.format(schedule_at)}</code> ({f"через {schedule.format_delta(schedule_delta)})" if is_scheduled else "сейчас"})

Рассылку получат до {count} пользователей бота, она продлиться примерно {schedule.format_delta(duration)}.
            """
        )

    async def status_message(self, bc: BroadcastDB):
        info = await self.target.info(bc.id)
        if bc.done:
            return f"""
<b>Статус рассылки #{bc.id}:</b> ЗАВЕРШЕНА

Отправлено: {info.success}
Ошибок: {info.error}

Запущена в {schedule.format(bc.schedule or bc.created_at)}
Завершена в {schedule.format(bc.finished_at)}
(продлилась {schedule.format_delta((bc.schedule or bc.created_at)-bc.finished_at)})
            """

        left = info.total - info.processed
        if bc.forward:
            duration = self.forward_delay * left * len(bc.messages)
        else:
            duration = self.copy_delay * left * len(bc.messages)
        time_end = datetime.now() + duration
        return f"""
<b>Статус рассылки #{bc.id}:</b> В ПРОЦЕССЕ

Обработано: {info.processed} из {info.total}
Отправлено: {info.success}
Ошибок: {info.error}

Запущена в {schedule.format(bc.schedule or bc.created_at)}
Ориентировочное время завершения: {schedule.format(time_end)}
        """

    async def report_progress(self, bc: BroadcastDB):
        text = await self.status_message(bc)
        if bc.id not in self.status_messages:
            sent = []
            for chat_id in self.admins:
                try:
                    m = await self.bot.send_message(text)
                except Exception:
                    continue
                sent.append(StatusMessage(m.chat.id, m.message_id, m.date))
                await asyncio.sleep(0.1)
            self.status_messages[bc.id] = sent
            return

        for s in self.status_messages[bc.id]:
            if datetime.now() - s.updated_at < timedelta(minutes=1):
                continue
            try:
                m = await self.bot.edit_message_text(text, s.chat_id, s.message_id)
            except Exception:
                continue
            s.updated_at = m.date

    async def finished(self, bc: BroadcastDB):
        text = await self.status_message(bc)
        for chat_id in self.admins:
            try:
                await self.bot.send_message(text)
                # todo: отправлять списки, возможно csv
            except Exception:
                continue
            await asyncio.sleep(0.1)

    async def send_one_message(
        self,
        message: types.Message,
        chat_id: int,
        forward: bool,
        disable_web_page_preview: bool,
    ) -> types.Message:
        message.bot = self.bot
        if forward:
            await asyncio.sleep(self.forward_delay)
            return await message.forward(chat_id)

        await asyncio.sleep(self.copy_delay)
        return await message.send_copy(
            chat_id, disable_web_page_preview=disable_web_page_preview
        )

    async def send_messages(
        self,
        messages: list,
        chat_id: int,
        forward: bool,
        disable_web_page_preview: bool,
    ) -> list:
        sent = []

        for message in messages:
            if not isinstance(message, types.Message):
                message = types.Message.to_object(message)

            m = await self.send_one_message(
                message, chat_id, forward, disable_web_page_preview
            )
            sent.append(m)

        return sent

    async def process_targets(
        self,
        targets: typing.List[target.Item],
        messages: list,
        forward: bool,
        disable_web_page_preview: bool,
    ) -> typing.List[target.Item]:
        for t in targets:
            try:
                m = await self.send_messages(
                    messages, t.id, forward, disable_web_page_preview
                )
                t.success = True
                t.message_id = m.message_id
                t.date = m.date
            except asyncio.CancelledError:
                raise
            except Exception as e:
                t.success = False
                t.error = repr(e)
                t.date = datetime.now()
            t.processed = True
        return targets

    async def sender(self):
        bc = await self.storage.get_first_running()
        chunk_size = 10 if bc.forward else 100
        targets = await self.target.get(bc.id, chunk_size)

        if not targets:
            bc.done = True
            bc.finished_at = datetime.now()
            await self.storage.update(bc.id, bc)
            await self.finished(bc)
            return 0

        try:
            targets = await self.process_targets(
                targets, bc.messages, bc.forward, bc.disable_web_page_preview
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await self.target.update(bc.id, targets)
        asyncio.create_task(self.report_progress(bc))
        return len(targets)

    async def daemon(self):
        while True:
            try:
                count = await self.sender()
                if not count:
                    await asyncio.sleep(3)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass