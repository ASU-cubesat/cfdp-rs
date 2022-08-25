use crossbeam_channel::{Receiver, Sender};

use crate::pdu::{VariableID, PDU};

pub trait PDUTransport {
    type Config;
    type Err;

    /// Verify underyling communication method is ready.
    fn is_ready(&self) -> bool;

    /// Connect to all remote sources.
    fn connect(&self, config: Self::Config) -> Result<(), Self::Err>;

    /// Disconnect from remote sources.
    fn disconnect(&self) -> Result<(), Self::Err>;

    /// Bind to the local port/socket/etc
    /// Begin the listener thread
    fn bind(&self) -> Result<(), Self::Err>;

    /// Unbind from the local port/socket/etc
    /// stop listener thread
    fn unbind(&self) -> Result<(), Self::Err>;

    /// Send input PDU to the remote
    fn request(&self, destination_id: VariableID, pdu: PDU) -> Result<(), Self::Err>;

    /// Provides logic for listening for incoming PDUs and sending any outbound PDUs
    /// This function is designed to run in a thread in the background
    /// for any implementations of this Trait.
    /// A transport implementation will send any received messages through the
    /// [Sender] channel to the [Daemon](crate::daemon::Daemon).
    /// The [Daemon](crate::daemon::Daemon) is responsible for receiving messages and ditribute them to each
    /// [Transaction](crate::transaction::Transaction) as necessary.
    fn pdu_handler(
        &self,
        sender: Sender<Vec<u8>>,
        recv: Receiver<(VariableID, PDU)>,
    ) -> Result<(), Self::Err>;
}
