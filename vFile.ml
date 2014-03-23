open Lwt
exception Unsupported

module type Ops = sig
  open Lwt_io
  val fd: Uri.t -> Lwt_unix.file_descr Lwt.t
  val supported_schemes: unit -> string list
end


let drivers : (string, (module Ops)) Hashtbl.t = Hashtbl.create 13
let add_driver scheme driver = Hashtbl.add drivers scheme driver

let scheme uri = match Uri.scheme uri with
  | None -> "file"
  | Some scheme -> scheme

let try_open uri drv =
  let module File = (val drv : Ops) in
  try_lwt
    lwt fd = File.fd uri in
    return (Some fd)
  with Unsupported -> return None

let fd uri =
  let drivers = Hashtbl.find_all drivers (scheme uri) in
  lwt r = Lwt_list.fold_left_s (fun res drv -> match res with
      | None -> try_open uri drv
      | opened -> return opened) None drivers in
  match r with
  | None -> fail Unsupported
  | Some ch -> return ch

let supported_schemes () =
  Hashtbl.fold (fun k _ ks -> k::ks) drivers []


module Regular = struct
  let fd uri =
    Lwt_unix.(openfile (Uri.path uri) [O_RDONLY] 0o640)
  let supported_schemes () = ["file"]
end

module Socket = struct
  let demand what from = match what from with
    | Some r -> r
    | None -> raise Unsupported

  let addr_of_uri uri =
    let open Unix in
    let resolve_name domain socket_type uri =
      let host = demand Uri.host uri in
      let port = string_of_int (demand Uri.port uri) in
      match_lwt Lwt_unix.getaddrinfo host port
                  [AI_FAMILY domain; AI_SOCKTYPE socket_type] with
      | info::_ -> return (domain, socket_type, info.ai_addr)
      | [] -> raise Unsupported in
    lwt addr = match Uri.scheme uri with
      | Some "tcp"  -> resolve_name PF_INET SOCK_STREAM uri
      | Some "udp"  -> resolve_name PF_INET SOCK_DGRAM  uri
      | Some "unix" ->
        return (PF_UNIX, SOCK_STREAM, ADDR_UNIX (Uri.path uri))
      | _ -> raise Unsupported in
    return addr

  let fd uri =
    lwt domain, socket_type, addr = addr_of_uri uri in
    let fd = Lwt_unix.socket domain socket_type 0 in
    Lwt_unix.connect fd addr >> return fd

  let supported_schemes () = ["tcp"; "udp"; "unix"]
end

let register drv =
  let module File = (val drv : Ops) in
  List.iter (fun s -> add_driver s drv) (File.supported_schemes ())

let () =
  register (module Regular);
  register (module Socket)

