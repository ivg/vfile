exception Unsupported

module type Ops = sig
  open Lwt_io
  val fd: Uri.t -> Lwt_unix.file_descr Lwt.t
  val supported_schemes: unit -> string list
end

include Ops

val register: (module Ops) -> unit
