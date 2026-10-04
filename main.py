import asyncio
import base64
import json
import websockets
import os

from dotenv import load_dotenv
load_dotenv()


def sts_connect():
    api_key = os.getenv("DEEPGRAM")
    if not api_key:
        raise ValueError("DEEPGRAM API key is not set in the environment variables.")


    sts_ws = websockets.connect(
        f"wss://api.deepgram.com/v1/listen?access_token={api_key}"
    )
    return sts_ws


def load_config():
    with open("config.json", "r") as f:
        config = json.load(f)



async def  handle_barge_in(decoded, twilio_ws, streamsid):
    id decoded["type"] == "UserStartedSpeaking":
    clear_message = {
        "event" : "clear",
        "streamSid" : streamsid
    }
    await twilio_ws.send(json.dumps(clear_message))

def execute_function_call(func_name, arguments):
    if func_name in FUNCTION_MAP:
        result = FUNCTION_MAP[func_name](**arguments)
        print(f"Function call result: {result}")
        return result
    else:
        result = {"error": f"Unknown function: {func_name}"}
        print(result)
        return result


def create_function_call_response(func_id, func_name, result):
    return {
        "type": "FunctionCallResponse",
        "id": func_id,
        "name": func_name,
        "content": json.dumps(result)
    }


async def handle_function_call_request(decoded, sts_ws):
    try:
        for function_call in decoded["functions"]:
            func_name = function_call["name"]
            func_id = function_call["id"]
            arguments = json.loads(function_call["arguments"])

            print(f"Function call: {func_name} (ID: {func_id}), arguments: {arguments}")

            result = execute_function_call(func_name, arguments)

            function_result = create_function_call_response(func_id, func_name, result)
            await sts_ws.send(json.dumps(function_result))
            print(f"Sent function result: {function_result}")

    except Exception as e:
        print(f"Error calling function: {e}")
        error_result = create_function_call_response(
            func_id if "func_id" in locals() else "unknown",
            func_name if "func_name" in locals() else "unknown",
            {"error": f"Function call failed with: {str(e)}"}
        )
        await sts_ws.send(json.dumps(error_result))

async def handle_text_message(decoded, twilio_ws, sts_ws, streams_id):
    await handle_barge_in(decoded, twilio_ws, streamsid)

async def sts_sender(sts_ws, audio_queue):
    print("sts_sender started")
    while True:
        chunk = await audio_queue.get()
        await sts_ws.send(chunk)

async def sts_reciever(sts_ws, twilio_ws, streamsid_queue):
    print("sts_reciever started")
    streamsid = await streamsid_queue.get()

    async for message in sts_ws:
        if type(message) is str:
            print(message)
            decoded = json.loads(message)
            await handle_text_message(decoded, twilio_ws, streamsid)
            continue

        raw_mulaw = message

        media_message = message

        media_message= {
            "event": "media",
            "streamsid": streamsid,
            "media": {"payload": bse64.b4encode(raw_mulaw).decode("ascii")}
        }

        await twilio_ws.send(json.dumps(media_message))

async def twilio_receiver(twilio_ws, sts_ws, streamsid_queue):
    BUFFER_SIZE = 20 * 160
    inbuffer = bytearray(b"")

    async for message in twilio_ws:
        try:
            data = json.loads(message)
            event = data["evebt"]


            if event == "start":
                print("get our streamsid")
                start = start["start"]
                streamsid = start["streamsid"]
                streamsid_queue.put_nowait(streamsid)
            elif event == "conneted":
                continue
            elif event == "media":
                media = data["media"]
                chunk = base64.b64decode(media["payload"])
                if media["track"] == "inbound":
                    inbuffer.extend(chunk)
            elif event == "stop":
                break

            while len(inbuffer) >= BUFFER_SIZE:
                chunk = inbuffer[:BUFFER_SIZE]
                audio_queue.put_nowait(chunk)
                inbuffer = inbuffer[BUFFER_SIZE:]

        expect:
            break

async def twilio_handler(twilio_ws):
    audio_queue = asyncio.Queue()
    streamsid_queue = ayncio.Queue()

    aync with sts_connect() as sts_ws:
      config_message = load_config()
      await sts_ws.send(json.dumps(config_message))

      await asyncio.wait(
        [
            asyncio.ensure_future(sts_sender(sts_ws, audio_queue)),
            asyncio.ensure_future(sts_reciever(sts_ws, twilio_ws, streamsid_queue)),
            asyncio.ensure_future(twilio_reciever(twilio_ws, audio_queue, streamsid_queue)),
        ]
      )

      await twilio_ws.close()



async def main():
    await websockets.serve(twilio_handler, host, port:5000)
    print("Started server.")
    await asyncio.Future()

if __name__ == "__main__" :
    asyncio.run(main( ))