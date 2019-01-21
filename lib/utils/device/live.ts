import * as Bluebird from 'bluebird';
import * as chokidar from 'chokidar';
import * as Dockerode from 'dockerode';
import Livepush, { DockerfileActionGroup, FileUpdates } from 'livepush';
import * as _ from 'lodash';
import * as path from 'path';
import { Composition } from 'resin-compose-parse';
import { BuildTask } from 'resin-multibuild';

import Logger = require('../logger');

import DeviceAPI, { Status } from './api';

// How often do we want to check the device state
// engine has settled (delay in ms)
const DEVICE_STATUS_SETTLE_CHECK_INTERVAL = 500;

interface MonitoredContainer {
	context: string;
	livepush: Livepush;
	monitor: chokidar.FSWatcher;
}

interface ContextEvent {
	type: 'add' | 'change' | 'unlink';
	filename: string;
	serviceName: string;
}

export class LivepushManager {
	private lastDeviceStatus: Status | null = null;
	private containers: { [serviceName: string]: MonitoredContainer } = {};

	public constructor(
		private buildContext: string,
		private composition: Composition,
		private buildTasks: BuildTask[],
		private docker: Dockerode,
		private api: DeviceAPI,
		private logger: Logger,
	) {}

	public async init(): Promise<void> {
		this.logger.logLivepush('Waiting for device state to settle...');
		// The first thing we need to do is let the state 'settle',
		// so that all of the containers are running and ready to
		// be livepush'd into
		await this.awaitDeviceStateSettle();
		// Split the composition into a load of differents paths which we can
		// create livepush instances for
		for (const serviceName in this.composition.services) {
			const service = this.composition.services[serviceName];
			const buildTask = _.find(
				this.buildTasks,
				task => task.serviceName === serviceName,
			);

			if (buildTask == null) {
				throw new Error(
					`Could not find a build task for service: ${serviceName}`,
				);
			}

			// We don't need to care about images
			if (service.build != null) {
				const context = path.join(this.buildContext, service.build.context);
				const dockerfile = buildTask.dockerfile;
				if (dockerfile == null) {
					throw new Error(
						`Could not detect dockerfile for service: ${serviceName}`,
					);
				}

				// find the containerId from the device state
				const container = _.find(
					this.lastDeviceStatus!.containers,
					container => container.serviceName === serviceName,
				);
				if (container == null) {
					throw new Error(
						`Could not find container on-device for service: ${serviceName}`,
					);
				}

				const log = (msg: string) => {
					this.logger.logLivepush(`[service ${serviceName}] ${msg}`);
				};

				const livepush = new Livepush(
					dockerfile,
					context,
					container.containerId,
					this.docker,
				);

				livepush.on('commandExecute', (command: string) => {
					log(`Executing command: \`${command}\``);
				});
				livepush.on('commandOutput', output => {
					log(`    ${output.output.toString()}`);
				});
				livepush.on('containerRestart', () => {
					log(`Restarting container for service ${serviceName}`);
				});

				// TODO: Memoize this for containers which share a context
				const monitor = chokidar.watch('.', {
					// Get the files relative to the context
					cwd: context,
					// Don't emit events for initial detection of files
					ignoreInitial: true,
				});
				monitor.on('add', (path: string) =>
					this.handleFSEvent({ filename: path, type: 'add', serviceName }),
				);
				monitor.on('change', (path: string) =>
					this.handleFSEvent({ filename: path, type: 'change', serviceName }),
				);
				monitor.on('unlink', (path: string) =>
					this.handleFSEvent({ filename: path, type: 'unlink', serviceName }),
				);

				this.containers[serviceName] = {
					livepush,
					context,
					monitor,
				};
			}
		}
	}

	private async awaitDeviceStateSettle(): Promise<void> {
		// Cache the state to avoid unnecessary cals
		this.lastDeviceStatus = await this.api.getStatus();

		if (this.lastDeviceStatus.appState === 'applied') {
			return;
		}

		this.logger.logDebug(
			`Device state not settled, retrying in ${DEVICE_STATUS_SETTLE_CHECK_INTERVAL}ms`,
		);
		await Bluebird.delay(DEVICE_STATUS_SETTLE_CHECK_INTERVAL);
		await this.awaitDeviceStateSettle();
	}

	private async handleFSEvent(fsEvent: ContextEvent): Promise<void> {
		// TODO: If there's a dockerfile event, we must perform a rebuild
		this.logger.logDebug(
			`Got an filesystem event for service: ${
				fsEvent.serviceName
			}. Event: ${JSON.stringify(fsEvent)}`,
		);

		let updates: FileUpdates;
		switch (fsEvent.type) {
			case 'add':
				updates = new FileUpdates({
					updated: [],
					added: [fsEvent.filename],
					deleted: [],
				});
				break;
			case 'change':
				updates = new FileUpdates({
					updated: [fsEvent.filename],
					added: [],
					deleted: [],
				});
				break;
			case 'unlink':
				updates = new FileUpdates({
					updated: [],
					added: [],
					deleted: [fsEvent.filename],
				});
				break;
			default:
				throw new Error(`Unknown event: ${fsEvent.type}`);
		}

		// Work out if we need to perform any changes on this container
		const livepush = this.containers[fsEvent.serviceName].livepush;
		const actions = livepush.actionsNeeded(updates);

		if (actions.length === 0) {
			return;
		}

		this.logger.logLivepush(
			`Detected changes for container ${fsEvent.serviceName}, updating...`,
		);

		await this.executeActions(actions, updates, livepush, fsEvent.serviceName);
	}

	private async executeActions(
		actions: DockerfileActionGroup[],
		updates: FileUpdates,
		livepush: Livepush,
		serviceName: string,
	) {
		try {
			await livepush.performActions(updates, actions);
		} catch (e) {
			console.log('There was an error executing commands: ', e);
		}
		this.logger.logLivepush(`Restarting container for service ${serviceName}`);
	}
}

export default LivepushManager;
