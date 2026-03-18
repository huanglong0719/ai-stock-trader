
import sys
import os
import asyncio
import argparse

# Add backend to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.chat_service import chat_service

async def main():
    parser = argparse.ArgumentParser(description="Developer Chat CLI for AI Trader")
    parser.add_argument("message", nargs="*", help="The message to send")
    parser.add_argument("--history", action="store_true", help="Show chat history")
    args = parser.parse_args()

    # Show History
    if args.history:
        print("\n--- Chat History ---")
        history = await chat_service.get_history(limit=20)
        for msg in history:
            role_color = "\033[94m" if msg.role == "user" else "\033[92m" # Blue for user, Green for AI
            reset_color = "\033[0m"
            print(f"{role_color}[{msg.role.upper()}]{reset_color} ({msg.created_at.strftime('%m-%d %H:%M')}): {msg.content}")
        print("--------------------\n")
        if not args.message:
            return

    # Send Message
    if args.message:
        content = " ".join(args.message)
        print(f"Developer (You) sending: {content}")
        
        # We assume the developer is sending this
        # Prefixing with [Developer] to help AI context if needed, though system prompt handles it.
        # But user messages are just stored as 'user'. 
        # Let's just send it. The AI will infer from context or we can explicitely say "我是开发者"
        
        try:
            print("AI is thinking...")
            response = await chat_service.process_user_message(content)
            print(f"\n\033[92m[AI TRADER]:\033[0m {response}\n")
        except Exception as e:
            print(f"Error: {e}")
    else:
        # Interactive Mode
        print("--- AI Trader Developer Console (Type 'exit' to quit) ---")
        while True:
            try:
                user_input = input("\033[94m[Developer] > \033[0m")
                if user_input.lower() in ['exit', 'quit']:
                    break
                if not user_input.strip():
                    continue
                
                print("AI is thinking...")
                response = await chat_service.process_user_message(user_input)
                print(f"\033[92m[AI TRADER]:\033[0m {response}\n")
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
