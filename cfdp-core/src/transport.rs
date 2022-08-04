use crate::pdu::{EntityID, PDU};

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
    fn request(&self, destination_id: EntityID, pdu: PDU) -> Result<(), Self::Err>;

    /// Provides logic for listening for incoming PDUs
    /// This function is designed to run in a thread in the background
    /// for any implementations of this Trait.
    /// The [Daemon] is responsible for receiving messages and ditribute them to each
    /// [Transaction](crate::transaction::Transaction) as necessary.
    fn incoming_pdu_handler(&self) -> Result<(), Self::Err>;

    //  Propagates any PDU up from the listener thread to the main Enginge
    // fn propagate_pdu(&self, pdu: PDU) -> Result<(), Self::Err>;
}
