import * as _ from 'lodash';
import * as sdk from 'balena-sdk';
const balena = sdk.fromSharedOptions();

export type DeviceContainerInfo = {
	status: string;
	services: {
		[name: string]: string;
	};
};

export const getDeviceContainerInfo = (uuid: string) => {
	console.log(`Getting container info for ${uuid}...`);
	return balena.settings.get('apiUrl').then(baseUrl =>
		balena.pine
			.get<{ id: number; uuid: string }>({
				resource: 'device',
				options: {
					$top: 1,
					$filter: {
						uuid,
					},
				},
			})
			.then(devices => {
				if (devices.length === 0) {
					throw new Error(`Device ${uuid} could not be found.`);
				}
				const [device] = devices;
				return balena.request.send({
					baseUrl,
					url: '/supervisor/v2/containerId',
					method: 'POST',
					body: {
						deviceId: device.id,
						method: 'GET',
					},
				});
			})
			.then(response => {
				if (!response.ok) {
					throw new Error(`Unable to get status for device ${uuid}`);
				}
				return response.body as DeviceContainerInfo;
			})
			.then(info => {
				return _.map(info.services, (containerId, name) => {
					return {
						name,
						containerId,
					};
				});
			}),
	);
};
