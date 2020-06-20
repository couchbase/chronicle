#!/usr/bin/env stack
-- stack --resolver lts-14.10 script
{-# OPTIONS_GHC -Wall -Werror -fno-warn-name-shadowing #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}

-- TODO: committed status
-- TODO: shun detection
-- TODO: non-voters
module Main
  ( main
  ) where

import Control.Monad (mapM_, when)
import Control.Monad.Extra ((&&^), ifM, whenM)
import Control.Monad.RWS.Strict (RWS, execRWS)
import qualified Control.Monad.State.Class as State
import qualified Control.Monad.Writer.Class as Writer
import Data.List ((\\), intersect, nub)
import Data.Map (Map, (!?))
import qualified Data.Map as Map
import Data.Maybe (fromJust, isJust)
import Text.Show.Pretty (ppShow)

type HistoryUID = String
type NodeId = String
type Term = (Int, NodeId)
type Seqno = Int

type VersionedConfig = (Term, Seqno, Config)
data Config
  = Stable [NodeId]
  | Transition [NodeId] [NodeId]
  deriving (Eq, Show)

data Msg =
  Msg
    { from :: NodeId
    , to :: NodeId
    , payload :: MsgPayload
    }
  deriving (Show)

data Cmd
  = CmdProvision HistoryUID
  | CmdSetNodes [NodeId]
  deriving (Eq, Show)

data CmdResult
  = CmdOk
  | CmdFailed
  deriving (Eq, Show)

data MsgPayload
  = Prepare HistoryUID Term
  | PrepareOk HistoryUID Term (Maybe VersionedConfig)
  | Accept HistoryUID Term VersionedConfig
  | AcceptOk HistoryUID Term VersionedConfig
  | PrepareBranch HistoryUID HistoryUID
  | PrepareBranchOk HistoryUID HistoryUID
  | Branch HistoryUID HistoryUID VersionedConfig
  | BranchOk HistoryUID HistoryUID VersionedConfig
  | CmdReq Cmd
  | CmdResp CmdResult
  deriving (Eq, Show)

data ProposerAction
  = None
  | ActionPrepare HistoryUID Term
  | ActionAccept HistoryUID Term VersionedConfig
  deriving (Eq, Show)

type NodeM a = RWS () [Msg] NodeState a

data NodeState =
  NodeState
    { historyUID :: Maybe HistoryUID
    , node :: NodeId
    , term :: Term
    , config :: Maybe VersionedConfig
      -- proposer state
    , cmds :: [(NodeId, Cmd)]
    , currentCmd :: Maybe (NodeId, Cmd)
    , currentAction :: ProposerAction
    , votes :: [NodeId]
    }
  deriving (Show)

configNodes :: Config -> [NodeId]
configNodes (Stable nodes) = nodes
configNodes (Transition oldNodes newNodes) = nub $ oldNodes ++ newNodes

self :: NodeM NodeId
self = State.gets node

send :: NodeId -> MsgPayload -> NodeM ()
send to payload = do
  msg <- Msg <$> self <*> pure to <*> pure payload
  Writer.tell [msg]

acceptor :: Msg -> NodeM ()
acceptor msg = acceptor' (from msg) (payload msg)

acceptor' :: NodeId -> MsgPayload -> NodeM ()
acceptor' node =
  \case
    (Prepare hid term) -> acceptorPrepare node hid term
    (Accept hid term config) -> acceptorAccept node hid term config
    (PrepareOk hid term config) -> acceptorPrepareOk node hid term config
    (AcceptOk hid term config) -> acceptorAcceptOk node hid term config
    (PrepareBranch oldHid newHid) -> acceptorPrepareBranch node oldHid newHid
    (PrepareBranchOk _oldHid _newHid) -> undefined
    (Branch oldHid newHid config) -> acceptorBranch node oldHid newHid config
    (BranchOk _oldHid _newHid _config) -> undefined
    (CmdReq cmd) -> acceptorCmdReq node cmd
    (CmdResp _) -> error "should not happen"

acceptorPrepare :: NodeId -> HistoryUID -> Term -> NodeM ()
acceptorPrepare node hid newTerm =
  checkHistory hid $ do
    NodeState {term, config} <- State.get
    when (newTerm > term) $ do
      adoptForeignTerm newTerm
      send node (PrepareOk hid newTerm config)

acceptorAccept :: NodeId -> HistoryUID -> Term -> VersionedConfig -> NodeM ()
acceptorAccept node hid newTerm newConfig@(newConfTerm, newConfSeqno, _) =
  checkHistory hid $ do
    NodeState {term, config} <- State.get
    if | newTerm > term -> doAdoptTerm >> doAccept
       | newTerm == term ->
         do case config of
              Nothing -> doAccept
              Just (ourTerm, ourSeqno, _)
                | newConfTerm >= ourTerm && newConfSeqno > ourSeqno -> doAccept
                | otherwise -> error "should not happen"
       | otherwise -> return ()
  where
    doAdoptTerm = adoptForeignTerm newTerm
    doAccept = do
      adoptConfig newConfig
      send node (AcceptOk hid newTerm newConfig)

checkHistory :: HistoryUID -> NodeM () -> NodeM ()
checkHistory hid cont = whenM (historyMatches hid) (setHistoryUID hid >> cont)

historyMatches :: HistoryUID -> NodeM Bool
historyMatches hid =
  getHistoryUID >>= \case
    Nothing -> return True
    Just ourHid -> return (ourHid == hid)

setTerm :: Term -> NodeM ()
setTerm term = State.modify (\state -> state {term})

adoptForeignTerm :: Term -> NodeM ()
adoptForeignTerm term = do
  clearCmds
  setTerm term

adoptConfig :: VersionedConfig -> NodeM ()
adoptConfig config = State.modify (\state -> state {config = Just config})

isOurTerm :: NodeM Bool
isOurTerm = do
  me <- self
  (_, termNode) <- State.gets term
  return $ me == termNode

startTerm :: NodeM ()
startTerm = do
  me <- self
  (termNumber, _) <- State.gets term
  let newTerm = (termNumber + 1, me)
  setTerm newTerm

  (_, _, config) <- fetchConfig
  hid <- fetchHistoryUID
  let nodes = configNodes config \\ [me]
  sendPrepare nodes
  setAction (ActionPrepare hid newTerm)
  addVote me
  checkPrepareQuorum

maybeStartPreparePhase :: NodeM Bool
maybeStartPreparePhase = ifM isOurTerm (return False) (startTerm >> return True)

sendPrepare :: [NodeId] -> NodeM ()
sendPrepare nodes = do
  term <- State.gets term
  hid <- fetchHistoryUID
  let msg = Prepare hid term
  mapM_ (flip send msg) nodes

startAcceptPhase :: NodeM ()
startAcceptPhase = do
  term <- State.gets term
  hid <- fetchHistoryUID
  config@(_, _, config') <- fetchConfig
  me <- self
  let nodes = configNodes config' \\ [me]
  let msg = Accept hid term config
  mapM_ (flip send msg) nodes
  setAction (ActionAccept hid term config)
  addVote me
  checkAcceptQuorum

proposeConfig :: Config -> NodeM ()
proposeConfig config = do
  term <- State.gets term
  (_, seqno, _) <- fetchConfig
  adoptConfig (term, seqno + 1, config)
  startAcceptPhase

clearCmds :: NodeM ()
clearCmds = do
  replyCmd CmdFailed
  cmds <- State.gets cmds
  mapM_ (\(c, _) -> send c (CmdResp CmdFailed)) cmds
  State.modify doClearCmds
  setAction None
  where
    doClearCmds state = state {cmds = []}

checkAction :: ProposerAction -> NodeM () -> NodeM ()
checkAction expectedAction cont = do
  NodeState {currentAction} <- State.get
  when (currentAction == expectedAction) cont

addVote :: NodeId -> NodeM ()
addVote vote = State.modify doAddVote
  where
    doAddVote state@(NodeState {votes}) = state {votes = vote : votes}

checkPrepareQuorum :: NodeM ()
checkPrepareQuorum =
  whenM haveQuorum $ do
    State.gets config >>= \case
      Nothing -> error "should not happen"
      Just _ -> startAcceptPhase

fetchConfig :: NodeM VersionedConfig
fetchConfig =
  State.gets config >>= \case
    Nothing -> error "should not happen"
    Just config -> return config

checkAcceptQuorum :: NodeM ()
checkAcceptQuorum =
  whenM haveQuorum $ do
    (_, _, config) <- fetchConfig
    case config of
      Stable _ -> replyCmd CmdOk >> setAction None >> maybeStartNextCmd
      Transition _ newNodes -> proposeConfig (Stable newNodes)

setAction :: ProposerAction -> NodeM ()
setAction action = State.modify doSetAction
  where
    doSetAction state = state {currentAction = action, votes = []}

haveQuorum :: NodeM Bool
haveQuorum = do
  NodeState {votes, config} <- State.get
  case config of
    Nothing -> error "should not happen"
    Just (_, _, config') -> return $ haveQuorumWithConfig votes config'

haveQuorumWithConfig :: [NodeId] -> Config -> Bool
haveQuorumWithConfig votes (Stable nodes) = haveQuorumWithNodes votes nodes
haveQuorumWithConfig votes (Transition oldNodes newNodes) =
  haveQuorumWithNodes votes oldNodes && haveQuorumWithNodes votes newNodes

haveQuorumWithNodes :: [NodeId] -> [NodeId] -> Bool
haveQuorumWithNodes votes nodes = 2 * numVotes > numNodes
  where
    numVotes = length (nub votes `intersect` nodes)
    numNodes = length nodes

prepareHandleNewConfig :: Maybe VersionedConfig -> NodeM Bool
prepareHandleNewConfig Nothing = return True
prepareHandleNewConfig (Just newConfig@(newTerm, newSeqno, newConfig')) = do
  NodeState {term, config} <- State.get
  if | newTerm > term ->
       adoptForeignTerm newTerm >> adoptConfig newConfig >> return False
     | Just (ourTerm, ourSeqno, config') <- config ->
       do when
            (newTerm > ourTerm || (newTerm == ourTerm && newSeqno > ourSeqno)) $ do
            let newNodes = configNodes newConfig' \\ configNodes config'
            sendPrepare newNodes
          return True
     | otherwise -> error "should not happen"

acceptorPrepareOk ::
     NodeId -> HistoryUID -> Term -> Maybe VersionedConfig -> NodeM ()
acceptorPrepareOk node hid term config =
  checkAction (ActionPrepare hid term) $ do
    addVote node
    whenM (prepareHandleNewConfig config) checkPrepareQuorum

acceptorAcceptOk :: NodeId -> HistoryUID -> Term -> VersionedConfig -> NodeM ()
acceptorAcceptOk node hid term config =
  checkAction (ActionAccept hid term config) $ addVote node >> checkAcceptQuorum

acceptorPrepareBranch :: NodeId -> HistoryUID -> HistoryUID -> NodeM ()
acceptorPrepareBranch _node _oldHid _newHid = undefined

acceptorBranch ::
     NodeId -> HistoryUID -> HistoryUID -> VersionedConfig -> NodeM ()
acceptorBranch = undefined

clearCmd :: NodeM ()
clearCmd = State.modify doClearCmd
  where
    doClearCmd state = state {currentCmd = Nothing}

replyCmd :: CmdResult -> NodeM ()
replyCmd reply =
  State.gets currentCmd >>= \case
    Nothing -> return ()
    Just (client, _cmd) -> send client (CmdResp reply) >> clearCmd

queueCmd :: NodeId -> Cmd -> NodeM ()
queueCmd client cmd = State.modify doQueueCmd
  where
    doQueueCmd state@(NodeState {cmds}) = state {cmds = cmds ++ [(client, cmd)]}

isIdle :: NodeM Bool
isIdle = do
  action <- State.gets currentAction
  return $ action == None

haveMoreCmds :: NodeM Bool
haveMoreCmds = not . null <$> State.gets cmds

peekCmd :: NodeM Cmd
peekCmd =
  State.gets cmds >>= \case
    [] -> error "shouldn't happen"
    (_, cmd):_ -> return cmd

pickUpCmd :: NodeM ()
pickUpCmd = do
  state@(NodeState {cmds}) <- State.get
  let cmd:cmds' = cmds
  State.put (state {currentCmd = Just cmd, cmds = cmds'})

pickUpAndReplyCmd :: CmdResult -> NodeM ()
pickUpAndReplyCmd reply = pickUpCmd >> replyCmd reply

startNextCmd :: NodeM ()
startNextCmd = do
  cmd <- peekCmd
  started <- startCmd cmd
  when started pickUpCmd

startCmd :: Cmd -> NodeM Bool
startCmd (CmdSetNodes newNodes) =
  rejectIfNotProvisioned $ startCmdSetNodes newNodes
startCmd (CmdProvision historyUID) = startCmdProvision historyUID

rejectIfNotProvisioned :: NodeM Bool -> NodeM Bool
rejectIfNotProvisioned cont =
  ifM isProvisioned cont (pickUpAndReplyCmd CmdFailed >> return False)

startCmdSetNodes :: [NodeId] -> NodeM Bool
startCmdSetNodes newNodes = do
  -- TODO: depending on new nodes we might have to establish more terms
  startedPrepare <- maybeStartPreparePhase
  if | startedPrepare -> return False
     | otherwise ->
       do (_, _, config) <- fetchConfig
          case config of
            Stable oldNodes -> proposeConfig (Transition oldNodes newNodes)
            Transition _ _ -> error "shouldn't happen"
          return True

isProvisioned :: NodeM Bool
isProvisioned = isJust <$> getHistoryUID

setHistoryUID :: HistoryUID -> NodeM ()
setHistoryUID historyUID = State.modify doSetHistoryUID
  where
    doSetHistoryUID state = state {historyUID = Just historyUID}

getHistoryUID :: NodeM (Maybe HistoryUID)
getHistoryUID = State.gets historyUID

fetchHistoryUID :: NodeM HistoryUID
fetchHistoryUID = fromJust <$> getHistoryUID

startCmdProvision :: HistoryUID -> NodeM Bool
startCmdProvision historyUID = do
  pickUpCmd
  ifM isProvisioned doReject doProvision
  return False
  where
    doReject = replyCmd CmdFailed
    doProvision = setHistoryUID historyUID >> replyCmd CmdOk

maybeStartNextCmd :: NodeM ()
maybeStartNextCmd = do
  whenM (isIdle &&^ haveMoreCmds) $ startNextCmd

acceptorCmdReq :: NodeId -> Cmd -> NodeM ()
acceptorCmdReq client cmd = queueCmd client cmd >> maybeStartNextCmd

emptyNode :: NodeId -> NodeState
emptyNode node =
  NodeState
    { historyUID = Nothing
    , node = node
    , term = term
    , config = Just (term, 0, Stable [node])
    , cmds = []
    , currentCmd = Nothing
    , currentAction = None
    , votes = []
    }
  where
    term = (0, "")

simulate :: [NodeState] -> [Msg] -> IO ()
simulate nodes msgs = simulate' nodes' msgs
  where
    nodes' = Map.fromList [(node s, s) | s <- nodes]

simulate' :: Map NodeId NodeState -> [Msg] -> IO ()
simulate' nodes [] = do
  putStrLn "no more messages"
  putStrLn $ "Final States:\n\n" ++ ppShow (map snd $ Map.toAscList nodes)
simulate' nodes (m:msgs) = do
  putStrLn $ "picked up a message:\n\n" ++ ppShow m ++ "\n\n"
  let toNode = to m
  case nodes !? toNode of
    Nothing ->
      putStrLn ("ignored the message to unknown node: " ++ toNode ++ "\n\n") >>
      simulate' nodes msgs
    Just node -> do
      let (node', newMsgs) = execRWS (acceptor m) () node
      let nodes' = Map.insert toNode node' nodes
      let msgs' = msgs ++ newMsgs
      putStrLn $ "state before:\n\n" ++ ppShow node ++ "\n\n"
      putStrLn $ "state after:\n\n" ++ ppShow node' ++ "\n\n"
      putStrLn $ "new messages:\n\n" ++ ppShow newMsgs ++ "\n\n"
      simulate' nodes' msgs'

main :: IO ()
main = do
  let a = emptyNode "a"
  let b = emptyNode "b"
  let c = emptyNode "c"
  simulate
    [a, b, c]
    [ (Msg "client" "a" (CmdReq (CmdProvision "history_a")))
    , (Msg "client" "b" (CmdReq (CmdProvision "history_b")))
    , (Msg "client" "a" (CmdReq (CmdSetNodes ["a", "b", "c"])))
    , (Msg "client" "b" (CmdReq (CmdSetNodes ["b", "c"])))
    , (Msg "client" "c" (CmdReq (CmdSetNodes ["a", "c"])))
    , (Msg "client" "c" (CmdReq (CmdSetNodes ["a", "c"])))
    , (Msg "client" "c" (CmdReq (CmdSetNodes ["a", "c"])))
    ]
