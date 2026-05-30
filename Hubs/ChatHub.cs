using Microsoft.AspNetCore.SignalR;
using ChatApp.Models;
using ChatApp.DataService;

namespace ChatApp.Hubs
{
    public class ChatHub : Hub
    {
        private readonly SharedDb _sharedDb;

        public ChatHub(SharedDb sharedDb)
        {
            _sharedDb = sharedDb;
        }

        public async Task JoinChatRoom(string userName, string chatRoom, string role)
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, chatRoom);
            await Groups.AddToGroupAsync(Context.ConnectionId, "announcements");

            _sharedDb.Connection[Context.ConnectionId] = new UserConnection
            {
                UserName = userName,
                ChatRoom = chatRoom,
                Role = role
            };

            await Clients.Group(chatRoom).SendAsync(
                "ReceiveMessage",
                "admin",
                $"{userName} joined as {role}"
            );
        }

        public async Task SendMessage(string chatRoom, string userName, string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            await Clients.Group(chatRoom).SendAsync("ReceiveMessage", userName, message);
        }

        public async Task SendAnnouncement(string userName, string message)
        {
            if (!_sharedDb.Connection.TryGetValue(Context.ConnectionId, out var user))
                return;

            if (user.Role != "Teacher")
                return;

            if (string.IsNullOrWhiteSpace(message))
                return;

            await Clients.Group("announcements").SendAsync(
                "ReceiveAnnouncement",
                userName,
                message
            );
        }

        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            if (_sharedDb.Connection.TryRemove(Context.ConnectionId, out var user))
            {
                await Clients.Group(user.ChatRoom).SendAsync(
                    "ReceiveMessage",
                    "admin",
                    $"{user.UserName} left the chat"
                );
            }

            await base.OnDisconnectedAsync(exception);
        }
    }
}