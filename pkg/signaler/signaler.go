package signaler

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/flutter-webrtc/flutter-webrtc-server/pkg/logger"
	"github.com/flutter-webrtc/flutter-webrtc-server/pkg/util"
	"github.com/flutter-webrtc/flutter-webrtc-server/pkg/websocket"
)

const (
	sharedKey = `flutter-webrtc-turn-server-shared-key`
)

type TurnCredentials struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	TTL      int      `json:"ttl"`
	Uris     []string `json:"uris"`
}

// Peer .
type Peer struct {
	info PeerInfo
	conn *websocket.WebSocketConn
}

// Session info.
type Session struct {
	id   string
	from Peer
	to   Peer
}

type Method string

const (
	New         Method = "new"
	Bye         Method = "bye"
	Offer       Method = "offer"
	Answer      Method = "answer"
	Candidate   Method = "candidate"
	Leave       Method = "leave"
	Keepalive   Method = "keepalive"
	ChatMessage Method = "chat_message"
)

type Request struct {
	Type Method      `json:"type"`
	Data interface{} `json:"data"`
}

type PeerInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	VisitDate string `json:"visitDate"`
	AppName   string `json:"appName"`
}

type Negotiation struct {
	From      string `json:"from"`
	To        string `json:"to"`
	SessionID string `json:"session_id"`
}

type Byebye struct {
	SessionID string `json:"session_id"`
	From      string `json:"from"`
}

type Error struct {
	Request string `json:"request"`
	Reason  string `json:"reason"`
}

type ChatMessageData struct {
	Type  string   `json:"type"`
	Peers []string `json:"peers"`
}

type Signaler struct {
	sync.RWMutex
	peers     map[string]Peer
	sessions  map[string]Session
	expresMap *util.ExpiredMap
	turnIp    string
}

func NewSignaler(ip string) *Signaler {
	var signaler = &Signaler{
		peers:     make(map[string]Peer),
		sessions:  make(map[string]Session),
		expresMap: util.NewExpiredMap(),
		turnIp:    ip}
	return signaler
}

// NotifyPeersUpdate .
func (s *Signaler) NotifyPeersUpdate(peers map[string]Peer) {
	s.RLock() // Блокируем на чтение перед итерацией
	infos := []PeerInfo{}
	for _, peer := range peers {
		infos = append(infos, peer.info)
	}

	request := Request{
		Type: "peers",
		Data: infos,
	}
	conns := make([]*websocket.WebSocketConn, 0, len(peers))
	for _, peer := range peers {
		conns = append(conns, peer.conn)
	}
	s.RUnlock() // Снимаем блокировку

	for _, c := range conns {
		s.Send(c, request)
	}
}

// HandleTurnServerCredentials .
// https://tools.ietf.org/html/draft-uberti-behave-turn-rest-00
func (s *Signaler) HandleTurnServerCredentials(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	params, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {

	}
	logger.Debugf("%v", params)
	service := params["service"][0]
	if service != "turn" {
		return
	}
	username := params["username"][0]
	ttl := 86400
	timestamp := time.Now().Unix() + int64(ttl)
	turnUsername := fmt.Sprintf("%d:%s", timestamp, username)
	hmac := hmac.New(sha1.New, []byte(sharedKey))
	hmac.Write([]byte(turnUsername))
	turnPassword := base64.RawStdEncoding.EncodeToString(hmac.Sum(nil))
	/*
		{
		     "username" : "12334939:mbzrxpgjys",
		     "password" : "adfsaflsjfldssia",
		     "ttl" : 86400,
		     "uris" : [
		       "turn:1.2.3.4:9991?transport=udp",
		       "turn:1.2.3.4:9992?transport=tcp",
		       "turns:1.2.3.4:443?transport=tcp"
			 ]
		}
		For client pc.
		var iceServer = {
			"username": response.username,
			"credential": response.password,
			"uris": response.uris
		};
		var config = {"iceServers": [iceServer]};
		var pc = new RTCPeerConnection(config);

	*/
	host := s.turnIp
	credential := TurnCredentials{
		Username: turnUsername,
		Password: turnPassword,
		TTL:      ttl,
		Uris: []string{
			// "turn:" + host + "?transport=udp",
			// "turn:" + host + "?transport=tcp",
			// "turns:" + host + "?transport=tcp",
			"turn:" + host + ":3478?transport=udp",
			"turn:" + host + ":3478?transport=tcp",
			"turns:" + host + ":5349?transport=tcp",
		},
	}
	s.expresMap.Set(turnUsername, credential, int64(ttl))
	json.NewEncoder(writer).Encode(credential)
}

func (s *Signaler) Send(conn *websocket.WebSocketConn, m interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		logger.Errorf(err.Error())
		return err
	}
	return conn.Send(string(data))
}

func (s *Signaler) HandleNewWebSocket(conn *websocket.WebSocketConn, request *http.Request) {
	logger.Infof("On Open remote=%s userAgent=%q path=%s", request.RemoteAddr, request.UserAgent(), request.URL.Path)
	conn.On("message", func(message []byte) {
		var body json.RawMessage
		request := Request{
			Data: &body,
		}
		err := json.Unmarshal(message, &request)
		if err != nil {
			logger.Errorf("Unmarshal error %v", err)
			return
		}

		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		if err != nil {
			logger.Errorf("Unmarshal error %v", err)
			return
		}

		switch request.Type {
		case New:
			var info PeerInfo
			err := json.Unmarshal(body, &info)
			if err != nil {
				logger.Errorf("Unmarshal login error %v", err)
				return
			}
			s.Lock()
			// Если пир с таким ID уже есть, закрываем его старое соединение
			if oldPeer, ok := s.peers[info.ID]; ok {
				logger.Infof("Replacing old connection for peer %s", info.ID)
				oldPeer.conn.Close()
			}
			s.peers[info.ID] = Peer{
				conn: conn,
				info: info,
			}
			//peersCopy := s.peers // Делаем копию для уведомления
			currentPeers := make(map[string]Peer)
			for k, v := range s.peers {
				currentPeers[k] = v
			}
			s.Unlock()
			s.NotifyPeersUpdate(currentPeers)
			break
		case Leave:
		case Offer:
			fallthrough
		case Answer:
			fallthrough
		case Candidate:
			{
				var negotiation Negotiation
				err := json.Unmarshal(body, &negotiation)
				if err != nil {
					logger.Errorf("Unmarshal "+string(request.Type)+" got error %v", err)
					return
				}
				if request.Type != Candidate {
					logger.Infof("Forward negotiation type=%s from=%s to=%s session=%s", request.Type, negotiation.From, negotiation.To, negotiation.SessionID)
				}
				to := negotiation.To
				peer, ok := s.peers[to]
				if !ok {
					logger.Warnf("Negotiation target not found type=%s from=%s to=%s session=%s", request.Type, negotiation.From, negotiation.To, negotiation.SessionID)
					msg := Request{
						Type: "error",
						Data: Error{
							Request: string(request.Type),
							Reason:  "Peer [" + to + "] not found ",
						},
					}
					s.Send(conn, msg)
					return
				}
				s.Send(peer.conn, request)
			}
			break
		case Bye:
			var bye Byebye
			err := json.Unmarshal(body, &bye)
			if err != nil {
				logger.Errorf("Unmarshal bye got error %v", err)
				return
			}
			logger.Infof("Bye received from=%s session=%s", bye.From, bye.SessionID)

			ids := strings.Split(bye.SessionID, ":")
			if len(ids) != 2 {
				msg := Request{
					Type: "error",
					Data: Error{
						Request: string(request.Type),
						Reason:  "Invalid session [" + bye.SessionID + "]",
					},
				}
				s.Send(conn, msg)
				return
			}

			sendBye := func(id string) {
				peer, ok := s.peers[id]

				if !ok {
					logger.Warnf("Bye target not found id=%s session=%s", id, bye.SessionID)
					msg := Request{
						Type: "error",
						Data: Error{
							Request: string(request.Type),
							Reason:  "Peer [" + id + "] not found.",
						},
					}
					s.Send(conn, msg)
					return
				}
				byeRequest := Request{
					Type: "bye",
					Data: map[string]interface{}{
						"to":         id,
						"session_id": bye.SessionID,
					},
				}
				logger.Infof("Send bye to=%s session=%s", id, bye.SessionID)
				s.Send(peer.conn, byeRequest)
			}

			// send to aleg
			sendBye(ids[0])
			//send to bleg
			sendBye(ids[1])

		case Keepalive:
			conn.RefreshDeadline(120 * time.Second)
			s.Send(conn, request)
			break
		case ChatMessage:
			{
				var chatData ChatMessageData
				err := json.Unmarshal(body, &chatData)
				if err != nil {
					logger.Errorf("Unmarshal chat_message got error %v", err)
					return
				}

				if chatData.Type != "notification" && chatData.Type != "message" {
					msg := Request{
						Type: "error",
						Data: Error{
							Request: string(request.Type),
							Reason:  "Invalid chat_message type. Expected notification or message.",
						},
					}
					s.Send(conn, msg)
					return
				}

				if len(chatData.Peers) == 0 {
					msg := Request{
						Type: "error",
						Data: Error{
							Request: string(request.Type),
							Reason:  "Peers list is empty.",
						},
					}
					s.Send(conn, msg)
					return
				}

				seen := make(map[string]struct{}, len(chatData.Peers))
				targetConns := make([]*websocket.WebSocketConn, 0, len(chatData.Peers))

				s.RLock()
				for _, peerID := range chatData.Peers {
					if _, ok := seen[peerID]; ok {
						continue
					}
					seen[peerID] = struct{}{}

					peer, ok := s.peers[peerID]
					if !ok {
						continue
					}
					targetConns = append(targetConns, peer.conn)
				}
				s.RUnlock()

				if len(targetConns) == 0 {
					msg := Request{
						Type: "error",
						Data: Error{
							Request: string(request.Type),
							Reason:  "No target peers found.",
						},
					}
					s.Send(conn, msg)
					return
				}

				for _, targetConn := range targetConns {
					s.Send(targetConn, request)
				}
			}
			break
		default:
			s.RLock()
			for _, peer := range s.peers {
				s.Send(peer.conn, request)
			}
			s.RUnlock()
			logger.Warnf("Unkown request %v", request)
		}
	})

	conn.On("close", func(code int, text string) {
		logger.Warnf("WebSocket close event code=%d text=%q", code, text)
		var peerID string
		var activeConns []*websocket.WebSocketConn

		s.Lock()
		// 1. Находим ID уходящего
		for id, p := range s.peers {
			if p.conn == conn {
				peerID = id
				break
			}
		}

		if peerID == "" {
			s.Unlock()
			logger.Warnf("Close event without registered peer code=%d text=%q", code, text)
			return
		}

		// 2. Удаляем его
		delete(s.peers, peerID)

		currentPeers := make(map[string]Peer)
		for k, v := range s.peers {
			currentPeers[k] = v // для NotifyPeersUpdate
			activeConns = append(activeConns, v.conn)
		}

		s.Unlock()

		leaveMsg := Request{Type: "leave", Data: peerID}
		logger.Warnf("Broadcasting leave message peer=%s activePeers=%d closeCode=%d closeText=%q", peerID, len(activeConns), code, text)
		for _, c := range activeConns {
			s.Send(c, leaveMsg)
		}

		// 3. Рассылаем обновленный список
		s.NotifyPeersUpdate(currentPeers)
	})
}
