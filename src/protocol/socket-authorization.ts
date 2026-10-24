import { calculateScramAuth, generateNonce } from "../security/sasl"
import { PgSocket, SocketConnector, SocketData } from "./socket-connector"
import { AuthenticationCodes, ResponseType, ResponseTypes } from "./constants"
import { encryptMd5 } from "../security/md5"
import { ErrCertificateFileNotFound, ErrDatabaseNotFound, ErrNonceMismatch, ErrPasswordRequired, ErrSocketFailedDuringAuth, ErrSSLDenied, ErrUntrustedCertificate, PostgresError } from "../error"
import { Future } from "fluent-future"
import { SSLMode } from "../types"
import { readFileSync } from "node:fs"
import { ConnectionRequestBuffer } from "./connection-request-writer"
import { ConnectionResponseBuffer } from "./connection-response-reader"


type CreateSocketConfig = {
    host: string
    port: number
}

export const socketHandlers = {
    data(socket: PgSocket, data: Buffer) {
        socket.data.onData?.(data)
    },
    close(socket: PgSocket) {
        socket.data.onClose?.()
    },
    error(socket: PgSocket, error: Error) {
        socket.data.onError?.(error)
    },
    drain(socket: PgSocket) {
        socket.data.onDrain?.()
    },
    handshake(socket: PgSocket, success: boolean, error: Error | null) {
        socket.data.onHandshake?.(socket, success, error)
    },
}


export function createSocket({ host, port }: CreateSocketConfig) {
    const { future, resolve, reject } = Future.withResolvers<PgSocket, PostgresError>()

    Bun.connect<SocketData>({
        hostname: host,
        port,
        data: {},
        socket: socketHandlers,
    }).then(
        socket => resolve(socket),
        () => reject(ErrDatabaseNotFound)
    )

    return future
}


type UpgradeSocketConfig = {
    ssl?: SSLMode
    caPath?: string
    host: string
}

export function upgradeSocket(socket: PgSocket, { ssl, caPath, host }: UpgradeSocketConfig) {
    if (ssl === 'disable') return Future.resolve(socket)

    const { future, reject, resolve } = Future.withResolvers<PgSocket, PostgresError>()

    const writer = ConnectionRequestBuffer.new(2048)

    socket.data = {
        onClose: () => reject(ErrSocketFailedDuringAuth),

        onData: data => {
            socket.data = {}

            const responseCode = data.toString('utf8', 0, 1)

            if (responseCode === 'S') { 
                if (data.length > 1) {
                    socket.terminate()
                    return reject(ErrSocketFailedDuringAuth)
                }

                let cert: Buffer | undefined = undefined

                try {
                    cert = caPath ? readFileSync(caPath) : undefined
                } catch {
                    socket.terminate()
                    return reject(ErrCertificateFileNotFound)
                }

                socket.upgradeTLS<SocketData>({
                    data: {
                        onHandshake: (tls, success) => {
                            if (!success) {
                                tls.terminate()
                                return reject(ErrUntrustedCertificate)
                            }

                            tls.data = {}
                            resolve(tls)
                        },
                        onError: () => reject(ErrUntrustedCertificate),
                        onClose: () => reject(ErrSocketFailedDuringAuth),
                    },
                    socket: socketHandlers,
                    tls: {
                        serverName: host,
                        rejectUnauthorized: ssl === 'require',
                        ca: cert,
                    },
                })

            } else if (responseCode === 'N') {
                if (ssl === 'require') {
                    socket.terminate()
                    reject(ErrSSLDenied)
                    return
                }
                resolve(socket)
            } else {
                socket.terminate()
                reject(ErrSocketFailedDuringAuth)
            }
        },
    }

    socket.write(writer.writeSSLRequest().asBuffer())
    writer.clear()

    return future
}


type AuthorizaSocketConfig = {
    password?: string
    user: string,
    database: string
}

export function authorizeSocket(socket: PgSocket, { password, user, database }: AuthorizaSocketConfig) {
    const { future, reject, resolve } = Future.withResolvers<PgSocket, PostgresError>()
    const writer = ConnectionRequestBuffer.new(2048)

    const nonce = generateNonce()
    let clientMessage = ''
    let serverMessage = ''


    const connector = new SocketConnector(
        socket, handle,
        () => reject(ErrSocketFailedDuringAuth)
    )

    function handle(type: ResponseType, length: number, reader: ConnectionResponseBuffer) {
        switch (type) {
            case ResponseTypes.Authentication: {
                authentication(length, reader)
            } break


            case ResponseTypes.ParamaterStatus: {
                reader.readParameterStatus()
            } break


            case ResponseTypes.ErrorResponse: {
                const error = reader.readErrorResponse()
                connector.close()
                reject(error)
            } return


            case ResponseTypes.BackendKeyData: {
                reader.readBackendKeyData()
            } break


            case ResponseTypes.ReadyForQuery: {
                reader.readReadyForQuery()

                resolve(connector.unwrapSocket())
            } return
        }
    }


    function authentication(length: number, reader: ConnectionResponseBuffer) {
        switch (reader.readAuthentication()) {
            case AuthenticationCodes.Ok: break


            case AuthenticationCodes.CleartextPassword: {
                if (!password) return reject(ErrPasswordRequired)
                connector.writeNowait(writer.writePassword(password))
                writer.clear()
            } break


            case AuthenticationCodes.MD5Password: {
                const salt = reader.readMD5Salt()
                if (!password) return reject(ErrPasswordRequired)

                connector.writeNowait(writer.writePassword(
                    encryptMd5(password, user, salt)
                ))
                writer.clear()
            } break


            case AuthenticationCodes.SASL: {
                reader.readSaslMechanisms()
                if (!password) return reject(ErrPasswordRequired)

                clientMessage = `n=${user},r=${nonce}`

                connector.writeNowait(writer.writeSaslInitial('SCRAM-SHA-256', `n,,${clientMessage}`))
                writer.clear()
            } break


            case AuthenticationCodes.SASLContinue: {
                serverMessage = reader.readSaslMessage(length)

                if (!password) return reject(ErrPasswordRequired)

                const parts = Object.fromEntries(serverMessage.split(',').map(x => x.split('=')))

                const serverNonce = parts.r
                const saltBase64 = parts.s
                const iterations = parseInt(parts.i, 10)

                if (!serverNonce.startsWith(nonce)) {
                    connector.close()
                    return reject(ErrNonceMismatch)
                }

                const clientFinalMessageWithoutProof = `c=biws,r=${serverNonce}`

                const authMessage = `${clientMessage},${serverMessage},${clientFinalMessageWithoutProof}`

                const { clientProof } = calculateScramAuth(password, saltBase64, iterations, authMessage)

                const clientFinalMessage = `${clientFinalMessageWithoutProof},p=${clientProof}`

                connector.writeNowait(writer.writeSaslResponse(clientFinalMessage))
                writer.clear()
            } break


            case AuthenticationCodes.SASLFinal: {
                reader.readSaslMessage(length)
            } break
        }
    }


    connector.writeNowait(writer.writeStartup(user, database))
    writer.clear()

    return future
}