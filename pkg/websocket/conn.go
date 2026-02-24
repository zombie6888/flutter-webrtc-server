package websocket

import (
	"errors"
	"sync"
	"time"

	"github.com/chuckpreslar/emission"
	"github.com/flutter-webrtc/flutter-webrtc-server/pkg/logger"
	"github.com/gorilla/websocket"
)

type WebSocketConn struct {
	emission.Emitter
	socket *websocket.Conn
	mutex  *sync.Mutex
	closed bool
}

func NewWebSocketConn(socket *websocket.Conn) *WebSocketConn {
	conn := &WebSocketConn{
		Emitter: *emission.NewEmitter(),
		socket:  socket,
		mutex:   new(sync.Mutex),
		closed:  false,
	}

	// Устанавливаем таймауты. Если клиент не пришлет ничего (даже Pong) за 60с - сокет закроется.
	socket.SetReadDeadline(time.Now().Add(60 * time.Second))
	socket.SetPongHandler(func(string) error {
		socket.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	
	return conn
}

func (conn *WebSocketConn) RefreshDeadline(timeout time.Duration) {
    conn.mutex.Lock()
    defer conn.mutex.Unlock()
    if !conn.closed {
        conn.socket.SetReadDeadline(time.Now().Add(timeout))
    }
}

func (conn *WebSocketConn) ReadMessage() {
// Гарантируем закрытие и очистку ресурсов
    defer conn.Close()

    for {
        messageType, message, err := conn.socket.ReadMessage()
        
        if err != nil {
            code := 1006
            if c, ok := err.(*websocket.CloseError); ok {
                code = c.Code
            }
            // Сообщаем сигналеру, что пир ушел, чтобы он удалил его из мапы
            conn.Emit("close", code, err.Error())
            return // Выходим из цикла
        }

        // Продлеваем дедлайн, так как получили реальные данные
        conn.RefreshDeadline(120 * time.Second)

        if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
            // Игнорируем пустые keepalive ("{}")
            if len(message) <= 2 && string(message) == "{}" {
                continue
            }
            conn.Emit("message", message)
        }
    }
}

func (conn *WebSocketConn) Send(message string) error {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	
	if conn.closed {
		return errors.New("websocket: write closed")
	}

	// Устанавливаем дедлайн на запись, чтобы медленные клиенты не вешали поток
	conn.socket.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := conn.socket.WriteMessage(websocket.TextMessage, []byte(message))
	if err != nil {
		logger.Errorf("Send error: %v", err)
	}
	return err
}

func (conn *WebSocketConn) Close() {
	conn.mutex.Lock()
	if conn.closed {
		conn.mutex.Unlock()
		return
	}
	conn.closed = true
	conn.mutex.Unlock()

	logger.Infof("Closing WebSocket connection")
	conn.socket.Close()
}