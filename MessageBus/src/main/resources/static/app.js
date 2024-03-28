const stompClient = new StompJs.Client({
    brokerURL: 'ws://localhost:8080/producer'
});

stompClient.onConnect = (frame) => {
    setConnected(true);
    console.log('Connected: ' + frame);
    stompClient.subscribe('/topic/acknowledgement/hello', (greeting) => {
        showGreeting(JSON.parse(greeting.body).messageId);
    });
};

stompClient.onWebSocketError = (error) => {
    console.error('Error with websocket', error);
};

stompClient.onStompError = (frame) => {
    console.error('Broker reported error: ' + frame.headers['message']);
    console.error('Additional details: ' + frame.body);
};

function setConnected(connected) {
    $("#connect").prop("disabled", connected);
    $("#disconnect").prop("disabled", !connected);
    if (connected) {
        $("#conversation").show();
    }
    else {
        $("#conversation").hide();
    }
    $("#greetings").html("");
}

function connect() {
    stompClient.activate();
}

function disconnect() {
    stompClient.deactivate();
    setConnected(false);
    console.log("Disconnected");
}

function sendName() {
        var messageId = document.getElementById('messageId').value;
        var messageType = document.getElementById('messageType').value;
        var message = document.getElementById('message').value;

        console.log("hello publish")
        stompClient.publish({
        destination: "/publish/hello",
        body: JSON.stringify({'messageID': messageId,
                             'key': messageType,
                             'message': message
                             })
        });
}

function showGreeting(message) {
    $("#greetings").append("<tr><td>" + message + "</td></tr>");
}

$(function () {
    $("form").on('submit', (e) => e.preventDefault());
    $( "#connect" ).click(() => connect());
    $( "#disconnect" ).click(() => disconnect());
    $( "#send" ).click(() => sendName());
});