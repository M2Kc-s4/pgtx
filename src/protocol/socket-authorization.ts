import { calculateScramAuth, generateNonce } from "../security/sasl"
import { SocketConnector } from "./socket-connector"
import { AuthenticationCodes, ResponseType, ResponseTypes } from "./constants"
import { encryptMd5 } from "../security/md5"
import { ErrCertificateFileNotFound, ErrDatabaseNotFound, ErrNonceMismatch, ErrPasswordRequired, ErrSocketFailedDuringAuth, ErrSSLDenied, ErrUntrustedCertificate, PostgresError } from "../error"
import { Future } from "fluent-future"
import { connect } from "node:tls"
import { createConnection, Socket } from "node:net"
import { ConnectionConfig, ConnectionPartialConfig, SSLMode } from "../types"
import { readFileSync } from "node:fs"
import { ConnectionRequestBuffer } from "./connection-request-writer"
import { ConnectionResponseBuffer } from "./connection-response-reader"


type CreateSocketConfig = {
    host: string
    port: number
}

export function createSocket({ host, port }: CreateSocketConfig) {
    const { future, resolve, reject } = Future.withResolvers<Socket, PostgresError>()

    const socket = createConnection({host, port})

    socket.once('connect', () => resolve(socket))
    socket.once('error', () => reject(ErrDatabaseNotFound))

    return future
}


type UpgradeSocketConfig = {
    ssl?: SSLMode
    caPath?: string
    host: string
}

export function upgradeSocket(socket: Socket, { ssl, caPath, host }: UpgradeSocketConfig) {
    if (ssl === 'disable') return Future.resolve(socket)

    const { future, reject, resolve } = Future.withResolvers<Socket, PostgresError>()

    const writer = ConnectionRequestBuffer.new(2048)

    function onError() {
        reject(ErrSocketFailedDuringAuth)
    }

    socket.once('error', onError)

    socket.once('data', (data: Buffer) => {
        socket.off('error', onError)

        const responseCode = data.toString('utf8', 0, 1)

        if (responseCode === 'S') {
            if (data.length > 1) {
                socket.unshift(data.subarray(1))
            }

            let cert: Buffer | undefined = undefined

            try {
                cert = caPath ? readFileSync(caPath) : undefined
            } catch {
                socket.destroy()
                return reject(ErrCertificateFileNotFound)
            }

            const tls = connect({
                socket: socket,
                host: host,
                rejectUnauthorized: ssl === 'require',
                ca: cert
            })

            const tlsCleanup = () => {
                tls.off('secureConnect', onSecureConnect)
                tls.off('error', onTlsError)
            }

            const onSecureConnect = () => {
                tlsCleanup()                
                resolve(tls)
            }

            const onTlsError = () => {
                tlsCleanup()
                tls.destroy() 
                reject(ErrUntrustedCertificate)
            }

            tls.once('secureConnect', onSecureConnect)
            tls.once('error', onTlsError)

        } else if (responseCode === 'N') {
            if (ssl === 'require') {
                socket.destroy()
                reject(ErrSSLDenied)
                return
            }
            resolve(socket)
        } else {
            socket.destroy()
            reject(ErrSocketFailedDuringAuth)
        }
    })

    socket.write(writer.writeSSLRequest().asBuffer())
    writer.clear()

    return future
}


type AuthorizaSocketConfig = {
    password?: string
    user: string,
    database: string
}

export function authorizeSocket(socket: Socket, { password, user, database }: AuthorizaSocketConfig) {
    const { future, reject, resolve } = Future.withResolvers<Socket, PostgresError>()
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