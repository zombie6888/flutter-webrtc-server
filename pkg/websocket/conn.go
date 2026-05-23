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
	socket         *websocket.Conn
	mutex          *sync.Mutex
	closed         bool
	createdAt      time.Time
	lastReadAt     time.Time
	lastWriteAt    time.Time
	readDeadlineAt time.Time
}

func NewWebSocketConn(socket *websocket.Conn) *WebSocketConn {
	now := time.Now()
	conn := &WebSocketConn{
		Emitter:        *emission.NewEmitter(),
		socket:         socket,
		mutex:          new(sync.Mutex),
		closed:         false,
		createdAt:      now,
		lastReadAt:     now,
		lastWriteAt:    now,
		readDeadlineAt: now.Add(60 * time.Second),
	}

	// Устанавливаем таймауты. Если клиент не пришлет ничего (даже Pong) за 60с - сокет закроется.
	socket.SetReadDeadline(conn.readDeadlineAt)
	socket.SetPongHandler(func(string) error {
		deadline := time.Now().Add(60 * time.Second)
		conn.mutex.Lock()
		conn.lastReadAt = time.Now()
		conn.readDeadlineAt = deadline
		conn.mutex.Unlock()
		socket.SetReadDeadline(deadline)
		return nil
	})

	return conn
}

func (conn *WebSocketConn) RefreshDeadline(timeout time.Duration) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if !conn.closed {
		conn.readDeadlineAt = time.Now().Add(timeout)
		conn.socket.SetReadDeadline(conn.readDeadlineAt)
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
			conn.mutex.Lock()
			lastReadAt := conn.lastReadAt
			lastWriteAt := conn.lastWriteAt
			readDeadlineAt := conn.readDeadlineAt
			createdAt := conn.createdAt
			conn.mutex.Unlock()
			logger.Warnf(
				"WebSocket read error remote=%s code=%d err=%v age=%s lastRead=%s lastWrite=%s readDeadline=%s",
				conn.socket.RemoteAddr(),
				code,
				err,
				time.Since(createdAt).Round(time.Second),
				lastReadAt.Format(time.RFC3339),
				lastWriteAt.Format(time.RFC3339),
				readDeadlineAt.Format(time.RFC3339),
			)
			// Сообщаем сигналеру, что пир ушел, чтобы он удалил его из мапы
			conn.Emit("close", code, err.Error())
			return // Выходим из цикла
		}

		conn.mutex.Lock()
		conn.lastReadAt = time.Now()
		conn.mutex.Unlock()

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
	conn.lastWriteAt = time.Now()
	err := conn.socket.WriteMessage(websocket.TextMessage, []byte(message))
	if err != nil {
		logger.Errorf("WebSocket send error remote=%s err=%v bytes=%d", conn.socket.RemoteAddr(), err, len(message))
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

	conn.socket.Close()
}
